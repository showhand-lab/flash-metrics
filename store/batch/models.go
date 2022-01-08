package batch

import (
	"bytes"
	"context"
	"sync"

	"github.com/showhand-lab/flash-metrics/store/model"
)

type Task struct {
	WG    *sync.WaitGroup
	Ctx   context.Context
	ErrCh chan error
	Data  []*TimeSeries
}

type TimeSeries struct {
	*model.TimeSeries

	// an internal fields for store
	// used to organize label value by label id
	//
	// [ label0 ] [ label1 ] [ label2 ]  ...  [ label14 ]
	// [   v0    ,    v1    ,    v2    , ... ,    v14   ]
	sortedLabelValue []string

	// an internal fields for store
	tsid int64
}

func (ts *TimeSeries) marshalSortedLabel(buffer *bytes.Buffer) {
	buffer.WriteString(ts.Name)
	for _, v := range ts.sortedLabelValue {
		buffer.WriteByte('$')
		buffer.WriteString(v)
	}
}
