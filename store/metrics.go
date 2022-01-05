package store

type TimeSeries struct {
	Name    string
	Labels  []Label
	Samples []Sample
}

type Label struct {
	Name  string
	Value string
}

type Sample struct {
	TimestampMs int64
	Value       float64
}

type Matcher struct {
	LabelName  string
	LabelValue string
	IsRE       bool
	IsNegative bool
}

type MetricStorage interface {
	Store(timeSeries TimeSeries) error
	Query(startMs, endMs int64, metricsName string, matchers []Matcher) ([]TimeSeries, error)
}
