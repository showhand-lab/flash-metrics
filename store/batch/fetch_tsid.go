package batch

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/metas"
	"github.com/showhand-lab/flash-metrics-storage/table"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	timeSeriesSliceP = TimeSeriesSlicePool{}
	interfaceSliceP  = InterfaceSlicePool{}
	bufferP          = BufferPool{}
)

type FetchTSIDWorker struct {
	ctx context.Context

	cache *LRU
	meta  metas.MetaStorage
	db    *sql.DB

	fetchTSIDTasks    chan Task
	updateDateTasks   chan Task
	insertSampleTasks chan Task

	batchSize int
}

func NewFetchTSIDWorker(
	ctx context.Context,
	meta metas.MetaStorage,
	db *sql.DB,
	fetchTSIDTasks chan Task,
	updateDateTasks chan Task,
	insertSampleTasks chan Task,
	cache *LRU,
	batchSize int,
) *FetchTSIDWorker {

	return &FetchTSIDWorker{
		ctx: ctx,

		cache: cache,
		meta:  meta,
		db:    db,

		fetchTSIDTasks:    fetchTSIDTasks,
		updateDateTasks:   updateDateTasks,
		insertSampleTasks: insertSampleTasks,

		batchSize: batchSize,
	}
}

func (f *FetchTSIDWorker) Start() {
	for {
		select {
		case t := <-f.fetchTSIDTasks:
			if err := f.handleTask(t); err != nil {
				select {
				case t.ErrCh <- err:
				default:
				}
			}
			t.WG.Done()
		case <-f.ctx.Done():
			return
		}
	}
}

func (f *FetchTSIDWorker) handleTask(t Task) error {
	return f.splitBatch(f.batchSize, t.Data, func(batch []*TimeSeries) error {
		if err := f.batchFillSortedLabelValues(t.Ctx, batch); err != nil {
			return err
		}
		if err := f.batchFillTSID(t.Ctx, batch); err != nil {
			return err
		}
		newTask := Task{
			WG:    t.WG,
			Ctx:   t.Ctx,
			ErrCh: t.ErrCh,
			Data:  batch,
		}
		return f.scheduleToNextWorker(newTask)
	})
}

func (f *FetchTSIDWorker) scheduleToNextWorker(t Task) error {
	t.WG.Add(1)
	select {
	case f.updateDateTasks <- t:
	default:
		log.Warn("update date workers are busy, drop task")
		t.WG.Done()
		return errors.New("update date workers are busy")
	}

	t.WG.Add(1)
	select {
	case f.insertSampleTasks <- t:
	default:
		log.Warn("insert sample workers are busy, drop task")
		t.WG.Done()
		return errors.New("insert sample workers are busy")
	}
	return nil
}

func (f *FetchTSIDWorker) batchFillSortedLabelValues(ctx context.Context, timeSeries []*TimeSeries) error {
	for _, ts := range timeSeries {
		labelName := make([]string, 0, len(ts.Labels))
		for _, l := range ts.Labels {
			labelName = append(labelName, l.Name)
		}
		meta, err := f.meta.StoreMeta(ctx, ts.Name, labelName)
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

func (f *FetchTSIDWorker) batchFillTSID(ctx context.Context, timeSeries []*TimeSeries) error {
	now := time.Now()
	defer func() {
		log.Debug("batch fill tsids", zap.Duration("in", time.Since(now)), zap.Int("size", len(timeSeries)))
	}()

	buffer := bufferP.Get()
	defer bufferP.Put(buffer)

	slowPathTs := timeSeriesSliceP.Get()
	defer timeSeriesSliceP.Put(slowPathTs)

	f.cache.Lock()
	for _, ts := range timeSeries {
		buffer.Reset()
		ts.marshalSortedLabel(buffer)

		// fast path
		if v, ok := f.cache.Inner.Get(buffer.String()); ok {
			ts.tsid = v.(int64)
			continue
		}

		*slowPathTs = append(*slowPathTs, ts)
	}
	f.cache.Unlock()

	if len(*slowPathTs) == 0 {
		return nil
	}

	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)

	writeCount := 0
	var sb strings.Builder
	sb.WriteString("INSERT IGNORE INTO flash_metrics_index VALUES")
	for _, ts := range *slowPathTs {
		if writeCount != 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(" (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		*args = append(*args, ts.Name)
		for _, lv := range ts.sortedLabelValue {
			*args = append(*args, lv)
		}
		writeCount += 1
	}

	if _, err := f.db.ExecContext(ctx, sb.String(), *args...); err != nil {
		return err
	}

	readCount := 0
	*args = (*args)[:0]
	sb.Reset()
	sb.WriteString("SELECT t.tsid FROM (\n")
	for i, ts := range *slowPathTs {
		if readCount != 0 {
			sb.WriteString("UNION ALL\n")
		}
		sb.WriteString("SELECT ")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(" AS id, _tidb_rowid tsid FROM flash_metrics_index WHERE metric_name = ? ")
		*args = append(*args, ts.Name)
		for j, lv := range ts.sortedLabelValue {
			sb.WriteString("AND label")
			sb.WriteString(strconv.Itoa(j))
			sb.WriteString(" = ? ")
			*args = append(*args, lv)
		}
		readCount += 1
	}
	sb.WriteString(") t ORDER BY id")

	rows, err := f.db.QueryContext(ctx, sb.String(), *args...)
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
		f.cache.Lock()
		f.cache.Inner.Add(buffer.String(), tsid)
		f.cache.Unlock()
	}

	return nil
}

func (f *FetchTSIDWorker) splitBatch(batchSize int, timeSeries []*TimeSeries, accessBatches func([]*TimeSeries) error) error {
	begin := 0
	currentBatchSize := 0

	for i, t := range timeSeries {
		currentBatchSize += len(t.Samples)
		if currentBatchSize >= batchSize {
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
