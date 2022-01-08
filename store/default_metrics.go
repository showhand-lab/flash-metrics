package store

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/metas"
	"github.com/showhand-lab/flash-metrics-storage/store/batch"
	"github.com/showhand-lab/flash-metrics-storage/store/model"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultBatchSize = 500

	defaultFetchTSIDWorkers    = 8
	defaultUpdateDateWorkers   = 8
	defaultInsertSampleWorkers = 8
)

var (
	interfaceSliceP  = batch.InterfaceSlicePool{}
	timeSeriesSliceP = batch.TimeSeriesSlicePool{}
	timeSeriesP      = batch.TimeSeriesPool{}
)

type DefaultMetricStorage struct {
	metas.MetaStorage

	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	DB         *sql.DB
	batchTasks chan batch.Task
}

func NewDefaultMetricStorage(db *sql.DB) *DefaultMetricStorage {
	ctx, cancel := context.WithCancel(context.Background())

	ms := &DefaultMetricStorage{
		MetaStorage: metas.NewDefaultMetaStorage(db),
		ctx:         ctx,
		cancel:      cancel,
		DB:          db,
		batchTasks:  make(chan batch.Task, 1024),
	}

	updateDateTasks := make(chan batch.Task, 1024)
	insertSampleTasks := make(chan batch.Task, 1024)

	for i := 0; i < defaultInsertSampleWorkers; i++ {
		ms.wg.Add(1)
		worker := batch.NewInsertSampleWorker(
			ms.ctx,
			ms.DB,
			ms.batchTasks,
			insertSampleTasks,
		)
		go func() {
			worker.Start()
			ms.wg.Done()
		}()
	}

	for i := 0; i < defaultUpdateDateWorkers; i++ {
		ms.wg.Add(1)
		worker := batch.NewUpdateDateWorker(
			ms.ctx,
			ms.DB,
			updateDateTasks,
		)
		go func() {
			worker.Start()
			ms.wg.Done()
		}()
	}

	cache := batch.NewLRU(102400)
	for i := 0; i < defaultFetchTSIDWorkers; i++ {
		ms.wg.Add(1)
		worker := batch.NewFetchTSIDWorker(
			ms.ctx,
			ms.MetaStorage,
			ms.DB,
			ms.batchTasks,
			updateDateTasks,
			insertSampleTasks,
			cache,
			defaultBatchSize,
		)
		go func() {
			worker.Start()
			ms.wg.Done()
		}()
	}

	return ms
}

var _ MetricStorage = &DefaultMetricStorage{}

func (d *DefaultMetricStorage) Store(ctx context.Context, timeSeries model.TimeSeries) error {
	if len(timeSeries.Samples) == 0 {
		return nil
	}

	labelName := make([]string, 0, len(timeSeries.Labels))
	for _, l := range timeSeries.Labels {
		labelName = append(labelName, l.Name)
	}

	m, err := d.StoreMeta(ctx, timeSeries.Name, labelName)
	if err != nil {
		return err
	}

	// insert index
	if err = d.insertIndex(ctx, timeSeries, m); err != nil {
		return err
	}

	// get tsid
	tsid, err := d.getTSID(ctx, timeSeries, m)
	if err != nil {
		return err
	}

	// insert updated date
	if err = d.insertUpdatedDate(ctx, tsid, timeSeries); err != nil {
		return err
	}

	// insert data
	return d.insertData(ctx, tsid, timeSeries)
}

func (d *DefaultMetricStorage) BatchStore(ctx context.Context, timeSeries []*model.TimeSeries) error {
	now := time.Now()
	defer func() {
		log.Info("batch store", zap.Duration("in", time.Since(now)), zap.Int("size", len(timeSeries)))
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tss := timeSeriesSliceP.Get()
	defer func() {
		for _, ts := range *tss {
			timeSeriesP.Put(ts)
		}
		timeSeriesSliceP.Put(tss)
	}()

	for _, originalTS := range timeSeries {
		ts := timeSeriesP.Get()
		ts.TimeSeries = originalTS
		*tss = append(*tss, ts)
	}

	t := batch.Task{
		WG:    &sync.WaitGroup{},
		Ctx:   ctx,
		ErrCh: make(chan error),
		Data:  *tss,
	}

	t.WG.Add(1)
	select {
	case d.batchTasks <- t:
	default:
		log.Warn("fetch tsid workers are busy, drop task")
		t.WG.Done()
		return errors.New("fetch tsid workers are busy")
	}

	done := make(chan struct{})
	go func() {
		t.WG.Wait()
		close(done)
	}()

	select {
	case err := <-t.ErrCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// Query implements interface MetricStorage
//
// SELECT
//    tsid, label0, label1, CAST(UNIX_TIMESTAMP(ts)*1000 AS UNSIGNED) AS t, v
//  FROM
//    flash_metrics_index
//    INNER JOIN flash_metrics_update ON (_tidb_rowid = tsid)
//    INNER JOIN flash_metrics_data USING (tsid)
//  WHERE
//    metric_name = "xxx"
//    AND label0 != "yyy"
//    AND label1 REGEXP "zzz.*"
//    AND DATE(start_ts) <= updated_date AND updated_date <= DATE(end_ts)
//    AND start_ts <= ts AND ts <= end_ts
//  ORDER BY tsid, t;
func (d *DefaultMetricStorage) Query(ctx context.Context, start, end int64, metricsName string, matchers []model.Matcher) ([]model.TimeSeries, error) {
	m, err := d.QueryMeta(ctx, metricsName)
	if err != nil {
		return nil, err
	}

	// Check query label exists. If contains non-exist label in matchers, return empty set.
	for _, matcher := range matchers {
		if _, ok := m.Labels[metas.LabelName(matcher.LabelName)]; !ok {
			return nil, nil
		}
	}

	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)

	var sb strings.Builder
	sb.WriteString("SELECT tsid, ")
	names := make([]string, 0, len(m.Labels))
	for n, v := range m.Labels {
		sb.WriteString("label")
		sb.WriteString(strconv.Itoa(int(v)))
		sb.WriteString(", ")
		names = append(names, string(n))
	}
	sb.WriteString("CAST(UNIX_TIMESTAMP(ts)*1000 AS UNSIGNED) AS t, v\n")
	sb.WriteString(`
FROM
  flash_metrics_index
  INNER JOIN flash_metrics_update ON (_tidb_rowid = tsid)
  INNER JOIN flash_metrics_data USING (tsid)
WHERE
  metric_name = ?
`)
	*args = append(*args, metricsName)

	for _, matcher := range matchers {
		labelID := m.Labels[metas.LabelName(matcher.LabelName)]
		sb.WriteString("AND label")
		sb.WriteString(strconv.Itoa(int(labelID)))

		if matcher.IsRE {
			if matcher.IsNegative {
				sb.WriteString(" NOT REGEXP ?\n")
			} else {
				sb.WriteString(" REGEXP ?\n")
			}
		} else {
			if matcher.IsNegative {
				sb.WriteString(" != ?\n")
			} else {
				sb.WriteString(" = ?\n")
			}
		}
		*args = append(*args, matcher.LabelValue)
	}

	sb.WriteString("AND ? <= updated_date AND updated_date <= ?\n")
	*args = append(*args, time.Unix(start/1000, (start%1000)*1_000_000).UTC().Format("2006-01-02"))
	*args = append(*args, time.Unix(end/1000, (end%1000)*1_000_000).UTC().Format("2006-01-02"))
	sb.WriteString("AND ? <= ts AND ts <= ?\n")
	sb.WriteString("ORDER BY tsid, t;")
	*args = append(*args, time.Unix(start/1000, (start%1000)*1_000_000).UTC().Format("2006-01-02 15:04:05.999 -0700"))
	*args = append(*args, time.Unix(end/1000, (end%1000)*1_000_000).UTC().Format("2006-01-02 15:04:05.999 -0700"))

	rows, err := d.DB.QueryContext(ctx, sb.String(), *args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dest := interfaceSliceP.Get()
	defer interfaceSliceP.Put(dest)
	for i := 0; i < len(m.Labels)+3; i++ {
		*dest = append(*dest, nil)
	}

	destP := interfaceSliceP.Get()
	defer interfaceSliceP.Put(destP)
	for i := range *dest {
		*destP = append(*destP, &(*dest)[i])
	}

	var res []model.TimeSeries
	tsid := int64(0)
	var timeSeries *model.TimeSeries
	for rows.Next() {
		if err = rows.Scan(*destP...); err != nil {
			return nil, err
		}

		curTSID := (*dest)[0].(int64)
		if tsid != curTSID {
			tsid = curTSID
			res = append(res, model.TimeSeries{})
			timeSeries = &res[len(res)-1]
			timeSeries.Name = metricsName

			i := 1
			for _, name := range names {
				labelValue := string((*dest)[i].([]byte))
				if labelValue != "" {
					timeSeries.Labels = append(timeSeries.Labels, model.Label{
						Name:  name,
						Value: labelValue,
					})
				}

				i += 1
			}
		}

		ts := (*dest)[len(*dest)-2].(int64)
		v := (*dest)[len(*dest)-1].(float64)
		timeSeries.Samples = append(timeSeries.Samples, model.Sample{
			TimestampMs: ts,
			Value:       v,
		})
	}

	return res, nil
}

func (d *DefaultMetricStorage) Close() {
	d.cancel()
	d.wg.Wait()
}

// INSERT IGNORE INTO flash_metrics_index (metric_name, label0, label1) VALUES (?, ?, ?);
func (d *DefaultMetricStorage) insertIndex(ctx context.Context, timeSeries model.TimeSeries, m *metas.Meta) error {
	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)
	var sb strings.Builder

	sb.WriteString("INSERT IGNORE INTO flash_metrics_index (metric_name")
	*args = append(*args, timeSeries.Name)
	for _, label := range timeSeries.Labels {
		labelID := m.Labels[metas.LabelName(label.Name)]
		sb.WriteString(", label")
		sb.WriteString(strconv.Itoa(int(labelID)))
		*args = append(*args, label.Value)
	}
	sb.WriteString(") VALUES (?")
	for range timeSeries.Labels {
		sb.WriteString(", ?")
	}
	sb.WriteString(");")

	_, err := d.DB.ExecContext(ctx, sb.String(), *args...)
	return err
}

// SELECT _tidb_rowid FROM flash_metrics_index WHERE metric_name = ? AND label0 = ? AND label1 = ?;
func (d *DefaultMetricStorage) getTSID(ctx context.Context, timeSeries model.TimeSeries, m *metas.Meta) (int64, error) {
	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)
	var sb strings.Builder

	sb.WriteString("SELECT _tidb_rowid FROM flash_metrics_index WHERE metric_name = ?")
	*args = append(*args, timeSeries.Name)
	for _, label := range timeSeries.Labels {
		labelID := m.Labels[metas.LabelName(label.Name)]
		sb.WriteString("AND label")
		sb.WriteString(strconv.Itoa(int(labelID)))
		sb.WriteString(" = ? ")
		*args = append(*args, label.Value)
	}
	sb.WriteByte(';')
	row := d.DB.QueryRowContext(ctx, sb.String(), *args...)
	var res int64
	if err := row.Scan(&res); err != nil {
		return 0, err
	}
	return res, nil
}

// INSERT IGNORE INTO flash_metrics_update (tsid, updated_date) VALUES (?, ?), (?, ?), (?, ?);
func (d *DefaultMetricStorage) insertUpdatedDate(ctx context.Context, tsid int64, timeSeries model.TimeSeries) error {
	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)

	writeCount := 0
	var sb strings.Builder
	sb.WriteString("INSERT IGNORE INTO flash_metrics_update (tsid, updated_date) VALUES")

	dateMap := map[string]struct{}{}
	for k := range dateMap {
		delete(dateMap, k)
	}

	for _, sample := range timeSeries.Samples {
		date := time.Unix(sample.TimestampMs/1000, (sample.TimestampMs%1000)*1_000_000).UTC().Format("2006-01-02")
		dateMap[date] = struct{}{}
	}

	for k := range dateMap {
		if writeCount > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(" (?, ?)")
		*args = append(*args, tsid, k)
		writeCount += 1
	}

	if writeCount == 0 {
		return nil
	}

	_, err := d.DB.ExecContext(ctx, sb.String(), *args...)
	return err
}

// INSERT INTO flash_metrics_data (tsid, ts, v) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?);
func (d *DefaultMetricStorage) insertData(ctx context.Context, tsid int64, timeSeries model.TimeSeries) error {
	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)

	writeCount := 0
	var sb strings.Builder
	sb.WriteString("INSERT INTO flash_metrics_data (tsid, ts, v) VALUES")

	for _, sample := range timeSeries.Samples {
		if math.IsNaN(sample.Value) {
			continue
		}
		if writeCount != 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(" (?, ?, ?)")

		*args = append(*args, tsid)
		*args = append(*args, time.Unix(sample.TimestampMs/1000, (sample.TimestampMs%1000)*1_000_000).UTC().Format("2006-01-02 15:04:05.999 -0700"))
		*args = append(*args, sample.Value)
		writeCount += 1
	}

	if writeCount == 0 {
		return nil
	}

	_, err := d.DB.ExecContext(ctx, sb.String(), *args...)
	return err
}
