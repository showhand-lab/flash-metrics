package store

import "bytes"

type TimeSeries struct {
	Name    string
	Labels  []Label
	Samples []Sample

	// an internal fields for store
	// used to organize label value by label id
	//
	// [ label0 ] [ label1 ] [ label2 ]  ...  [ label14 ]
	// [   v0    ,    v1    ,    v2    , ... ,    v14   ]
	sortedLabelValue []string

	// an internal fields for store
	tsid int64
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

func (ts *TimeSeries) marshalSortedLabel(buffer *bytes.Buffer) {
	buffer.WriteString(ts.Name)
	for _, v := range ts.sortedLabelValue {
		buffer.WriteByte('$')
		buffer.WriteString(v)
	}
}
