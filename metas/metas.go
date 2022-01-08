package metas

import "context"

type MetaStorage interface {
	QueryMeta(ctx context.Context, metricName string) (*Meta, error)
	StoreMeta(ctx context.Context, metricName string, labelNames []string) (*Meta, error)
}
