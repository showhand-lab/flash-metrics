package store

import (
	"context"

	"github.com/showhand-lab/flash-metrics-storage/store/model"
)

type MetricStorage interface {
	Store(ctx context.Context, timeSeries model.TimeSeries) error
	BatchStore(ctx context.Context, timeSeries []*model.TimeSeries) error
	Query(ctx context.Context, startMs, endMs int64, metricsName string, matchers []model.Matcher) ([]model.TimeSeries, error)
	Close()
}
