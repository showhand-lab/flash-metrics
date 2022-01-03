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
	Value     float64
	Timestamp int64
}

type Matcher struct {
	LabelName  string
	LabelValue string
	IsLike     bool
	IsNegative bool
}

type MetricStorage interface {
	Store(timeSeries TimeSeries) error
	Query(start, end int64, metricsName string, matchers []Matcher) (*TimeSeries, error)
}
