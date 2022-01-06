package metas

type LabelName string
type LabelID int32

type Meta struct {
	MetricName string
	Labels     map[LabelName]LabelID
}
