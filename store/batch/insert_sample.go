package batch

import (
	"context"
	"database/sql"
	"math"
	"strings"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type InsertSampleWorker struct {
	ctx context.Context

	db *sql.DB

	fetchTSIDTasks    chan Task
	insertSampleTasks chan Task
}

func NewInsertSampleWorker(
	ctx context.Context,
	db *sql.DB,
	fetchTSIDTasks chan Task,
	insertSampleTasks chan Task,
) *InsertSampleWorker {
	return &InsertSampleWorker{
		ctx:               ctx,
		db:                db,
		fetchTSIDTasks:    fetchTSIDTasks,
		insertSampleTasks: insertSampleTasks,
	}
}

func (i *InsertSampleWorker) Start() {
	for {
		select {
		case t := <-i.insertSampleTasks:
			if err := i.handleTask(t); err != nil {
				select {
				case t.ErrCh <- err:
				default:
				}
			}
			t.WG.Done()
		case <-i.ctx.Done():
			return
		}
	}
}

func (i *InsertSampleWorker) handleTask(t Task) error {
	return i.insertSample(t.Ctx, t.Data)
}

func (i *InsertSampleWorker) insertSample(ctx context.Context, timeSeries []*TimeSeries) (err error) {
	now := time.Now()
	defer func() {
		log.Debug("batch insert sample", zap.Duration("in", time.Since(now)), zap.Int("size", len(timeSeries)))
	}()

	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)

	writeCount := 0
	var sb strings.Builder
	sb.WriteString("INSERT INTO flash_metrics_data (tsid, ts, v) VALUES")

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
		}
	}

	if writeCount == 0 {
		return nil
	}

	_, err = i.db.ExecContext(ctx, sb.String(), *args...)
	return err
}
