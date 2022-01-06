package store

import (
	"context"
	"database/sql"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/metas"
	"github.com/showhand-lab/flash-metrics-storage/table"

	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	defaultBatchSize       = 500
	defaultBatchSampleSize = 150
)

var (
	interfaceSliceP  = InterfaceSlicePool{}
	timeSeriesSliceP = TimeSeriesSlicePool{}
	bufferP          = BufferPool{}
)

type DefaultMetricStorage struct {
	*metas.DefaultMetaStorage

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	db *sql.DB
}

func NewDefaultMetricStorage(db *sql.DB) *DefaultMetricStorage {
	ctx, cancel := context.WithCancel(context.Background())
	return &DefaultMetricStorage{
		DefaultMetaStorage: metas.NewDefaultMetaStorage(db),

		ctx:    ctx,
		cancel: cancel,

		db: db,
	}
}

var _ MetricStorage = &DefaultMetricStorage{}

func (d *DefaultMetricStorage) Store(ctx context.Context, timeSeries TimeSeries) error {
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

var c, _ = lru.New(4096)

func (d *DefaultMetricStorage) BatchStore(ctx context.Context, timeSeries []*TimeSeries) error {
	return splitBatch(timeSeries, func(series []*TimeSeries) error {
		if err := d.batchFillSortedLabelValues(ctx, series); err != nil {
			return err
		}
		if err := d.batchFillTSID(ctx, series); err != nil {
			return err
		}
		// TODO: pipeline
		go func() {
			_ = d.batchInsertUpdateDate(ctx, series)
		}()
		return d.batchInsertSample(ctx, series)
	})
}

func splitBatch(timeSeries []*TimeSeries, accessBatches func([]*TimeSeries) error) error {
	begin := 0
	currentBatchSize := 0

	for i, t := range timeSeries {
		currentBatchSize += len(t.Samples)
		if currentBatchSize >= defaultBatchSize {
			if err := accessBatches(timeSeries[begin : i+1]); err != nil {
				return err
			}
			begin = i + 1
			currentBatchSize = 0
		}
	}

	if currentBatchSize != 0 {
		return accessBatches(timeSeries[begin:])
	}
	return nil
}

func (d *DefaultMetricStorage) batchFillSortedLabelValues(ctx context.Context, timeSeries []*TimeSeries) error {
	for _, ts := range timeSeries {
		labelName := make([]string, 0, len(ts.Labels))
		for _, l := range ts.Labels {
			labelName = append(labelName, l.Name)
		}
		meta, err := d.StoreMeta(ctx, ts.Name, labelName)
		if err != nil {
			return err
		}

		ts.sortedLabelValue = ts.sortedLabelValue[:0]
		for i := 0; i < table.MaxLabelCount; i++ {
			ts.sortedLabelValue = append(ts.sortedLabelValue, "")
		}
		for _, label := range ts.Labels {
			ts.sortedLabelValue[meta.Labels[metas.LabelName(label.Name)]] = label.Value
		}
	}

	return nil
}

func (d *DefaultMetricStorage) batchFillTSID(ctx context.Context, timeSeries []*TimeSeries) error {
	now := time.Now()
	defer func() {
		log.Debug("batch fill tsids", zap.Duration("in", time.Since(now)), zap.Int("size", len(timeSeries)))
	}()

	buffer := bufferP.Get()
	defer bufferP.Put(buffer)

	slowPathTs := timeSeriesSliceP.Get()
	defer timeSeriesSliceP.Put(slowPathTs)

	for _, ts := range timeSeries {
		buffer.Reset()
		ts.marshalSortedLabel(buffer)

		// fast path
		if v, ok := c.Get(buffer.String()); ok {
			ts.tsid = v.(int64)
			continue
		}

		*slowPathTs = append(*slowPathTs, ts)
	}

	if len(*slowPathTs) == 0 {
		return nil
	}

	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)

	var sb strings.Builder
	sb.WriteString("INSERT IGNORE INTO flash_metrics_index VALUES ")
	sb.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	*args = append(*args, (*slowPathTs)[0].Name)
	for _, lv := range (*slowPathTs)[0].sortedLabelValue {
		*args = append(*args, lv)
	}
	for _, ts := range (*slowPathTs)[1:] {
		sb.WriteString(", (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		*args = append(*args, ts.Name)
		for _, lv := range ts.sortedLabelValue {
			*args = append(*args, lv)
		}
	}

	if _, err := d.db.ExecContext(ctx, sb.String(), *args...); err != nil {
		return err
	}

	sb.Reset()
	sb.WriteString("SELECT _tidb_rowid FROM (\n")
	sb.WriteString("SELECT 0 AS id, _tidb_rowid FROM flash_metrics_index WHERE metric_name = ? ")
	*args = (*args)[:0]
	*args = append(*args, (*slowPathTs)[0].Name)
	for i, lv := range (*slowPathTs)[0].sortedLabelValue {
		sb.WriteString("AND label")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(" = ?\n")
		*args = append(*args, lv)
	}
	for i := 1; i < len(*slowPathTs); i++ {
		sb.WriteString("UNION ALL\n")
		sb.WriteString("SELECT ")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(" AS id, _tidb_rowid FROM flash_metrics_index WHERE metric_name = ? ")
		*args = append(*args, (*slowPathTs)[i].Name)
		for j, lv := range (*slowPathTs)[i].sortedLabelValue {
			sb.WriteString("AND label")
			sb.WriteString(strconv.Itoa(j))
			sb.WriteString(" = ?\n")
			*args = append(*args, lv)
		}
	}
	sb.WriteString(") ORDER BY id")

	rows, err := d.db.QueryContext(ctx, sb.String(), *args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for _, ts := range *slowPathTs {
		rows.Next()
		var tsid int64
		if err = rows.Scan(&tsid); err != nil {
			return err
		}
		ts.tsid = tsid

		buffer.Reset()
		ts.marshalSortedLabel(buffer)
		c.Add(buffer.String(), tsid)
	}

	return nil
}

func (d *DefaultMetricStorage) batchInsertUpdateDate(ctx context.Context, timeSeries []*TimeSeries) error {
	now := time.Now()
	defer func() {
		log.Debug("batch update date", zap.Duration("in", time.Since(now)), zap.Int("size", len(timeSeries)))
	}()

	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)

	writeCount := 0
	var sb strings.Builder
	sb.WriteString("INSERT IGNORE INTO flash_metrics_update (tsid, updated_date) VALUES")

	dateMap := map[string]struct{}{}
	for _, ts := range timeSeries {
		for k := range dateMap {
			delete(dateMap, k)
		}

		for _, sample := range ts.Samples {
			date := time.Unix(sample.TimestampMs/1000, (sample.TimestampMs%1000)*1_000_000).UTC().Format("2006-01-02")
			dateMap[date] = struct{}{}
		}

		for k := range dateMap {
			if writeCount > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(" (?, ?)")
			*args = append(*args, ts.tsid, k)
			writeCount += 1
		}
	}

	if writeCount == 0 {
		return nil
	}

	_, err := d.db.ExecContext(ctx, sb.String(), *args...)
	return err
}

func (d *DefaultMetricStorage) batchInsertSample(ctx context.Context, timeSeries []*TimeSeries) (err error) {
	now := time.Now()
	defer func() {
		log.Debug("batch insert sample", zap.Duration("in", time.Since(now)), zap.Int("size", len(timeSeries)))
	}()

	args := interfaceSliceP.Get()

	writeCount := 0
	var sb strings.Builder
	sb.WriteString("INSERT INTO flash_metrics_data (tsid, ts, v) VALUES")

	var wg sync.WaitGroup
	for _, ts := range timeSeries {
		for _, sample := range ts.Samples {
			if math.IsNaN(sample.Value) {
				continue
			}
			if writeCount != 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(" (?, ?, ?)")

			*args = append(*args, ts.tsid)
			*args = append(*args, time.Unix(sample.TimestampMs/1000, (sample.TimestampMs%1000)*1_000_000).UTC().Format("2006-01-02 15:04:05.999 -0700"))
			*args = append(*args, sample.Value)
			writeCount += 1

			if writeCount >= defaultBatchSampleSize {
				wg.Add(1)
				go func(query string, args []interface{}) {
					now := time.Now()
					defer func() {
						wg.Done()
						log.Debug("batch insert sample", zap.Duration("in", time.Since(now)), zap.Int("size", len(args)))
					}()
					_, err = d.db.ExecContext(ctx, query, args...)
				}(sb.String(), *args)

				sb.Reset()
				sb.WriteString("INSERT INTO flash_metrics_data (tsid, ts, v) VALUES")
				writeCount = 0
				args = interfaceSliceP.Get()
			}
		}
	}

	if writeCount > 0 {
		_, err = d.db.ExecContext(ctx, sb.String(), *args...)
	}

	interfaceSliceP.Put(args)
	wg.Wait()
	return err
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
func (d *DefaultMetricStorage) Query(ctx context.Context, start, end int64, metricsName string, matchers []Matcher) ([]TimeSeries, error) {
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

	rows, err := d.db.QueryContext(ctx, sb.String(), *args...)
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

	var res []TimeSeries
	tsid := int64(0)
	var timeSeries *TimeSeries
	for rows.Next() {
		if err = rows.Scan(*destP...); err != nil {
			return nil, err
		}

		curTSID := (*dest)[0].(int64)
		if tsid != curTSID {
			tsid = curTSID
			res = append(res, TimeSeries{})
			timeSeries = &res[len(res)-1]
			timeSeries.Name = metricsName

			i := 1
			for _, name := range names {
				labelValue := string((*dest)[i].([]byte))
				if labelValue != "" {
					timeSeries.Labels = append(timeSeries.Labels, Label{
						Name:  name,
						Value: labelValue,
					})
				}

				i += 1
			}
		}

		ts := (*dest)[len(*dest)-2].(int64)
		v := (*dest)[len(*dest)-1].(float64)
		timeSeries.Samples = append(timeSeries.Samples, Sample{
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
func (d *DefaultMetricStorage) insertIndex(ctx context.Context, timeSeries TimeSeries, m *metas.Meta) error {
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
	_, err := d.db.ExecContext(ctx, sb.String(), *args...)
	return err
}

// SELECT _tidb_rowid FROM flash_metrics_index WHERE metric_name = ? AND label0 = ? AND label1 = ?;
func (d *DefaultMetricStorage) getTSID(ctx context.Context, timeSeries TimeSeries, m *metas.Meta) (int64, error) {
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
	row := d.db.QueryRowContext(ctx, sb.String(), *args...)
	var res int64
	if err := row.Scan(&res); err != nil {
		return 0, err
	}
	return res, nil
}

// INSERT IGNORE INTO flash_metrics_update (tsid, updated_date) VALUES (?, ?), (?, ?), (?, ?);
func (d *DefaultMetricStorage) insertUpdatedDate(ctx context.Context, tsid int64, timeSeries TimeSeries) error {
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

	_, err := d.db.ExecContext(ctx, sb.String(), *args...)
	return err
}

// INSERT INTO flash_metrics_data (tsid, ts, v) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?);
func (d *DefaultMetricStorage) insertData(ctx context.Context, tsid int64, timeSeries TimeSeries) error {
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

	_, err := d.db.ExecContext(ctx, sb.String(), *args...)
	return err
}
