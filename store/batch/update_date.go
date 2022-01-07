package batch

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type UpdateDateWorker struct {
	ctx context.Context

	db *sql.DB

	updateDateTasks chan Task
}

func NewUpdateDateWorker(
	ctx context.Context,
	db *sql.DB,
	updateDateTasks chan Task,
) *UpdateDateWorker {
	return &UpdateDateWorker{
		ctx:             ctx,
		db:              db,
		updateDateTasks: updateDateTasks,
	}
}

func (u *UpdateDateWorker) Start() {
	for {
		select {
		case t := <-u.updateDateTasks:
			if err := u.handleTask(t); err != nil {
				select {
				case t.ErrCh <- err:
				default:
				}
			}
			t.WG.Done()
		case <-u.ctx.Done():
			return
		}
	}
}

func (u *UpdateDateWorker) handleTask(t Task) error {
	return u.batchUpdateDate(t.Ctx, t.Data)
	//return nil
}

func (u *UpdateDateWorker) batchUpdateDate(ctx context.Context, timeSeries []*TimeSeries) error {
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

	_, err := u.db.ExecContext(ctx, sb.String(), *args...)
	return err
}
