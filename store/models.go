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
