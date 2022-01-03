package metas

type LabelName string
type LabelID int32

type Meta struct {
	MetricName string
	Labels     map[LabelName]LabelID
}

type MetaStorage interface {
	QueryMeta(metricName string) (*Meta, error)
	StoreMeta(metricName string, labelNames []string) (*Meta, error)
}
