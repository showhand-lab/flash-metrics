package store

import "context"

type MetricStorage interface {
	Store(ctx context.Context, timeSeries TimeSeries) error
	BatchStore(ctx context.Context, timeSeries []TimeSeries) error
	Query(ctx context.Context, startMs, endMs int64, metricsName string, matchers []Matcher) ([]TimeSeries, error)
}
