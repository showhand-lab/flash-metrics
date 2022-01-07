package batch

import (
	"bytes"
	"sync"
)

type TimeSeriesPool struct {
	p sync.Pool
}

func (p *TimeSeriesPool) Get() *TimeSeries {
	v := p.p.Get()
	if v == nil {
		return &TimeSeries{}
	}
	return v.(*TimeSeries)
}

func (p *TimeSeriesPool) Put(v *TimeSeries) {
	v.sortedLabelValue = v.sortedLabelValue[:0]
	v.tsid = 0
	p.p.Put(v)
}

type TimeSeriesSlicePool struct {
	p sync.Pool
}

func (p *TimeSeriesSlicePool) Get() *[]*TimeSeries {
	v := p.p.Get()
	if v == nil {
		return &[]*TimeSeries{}
	}
	return v.(*[]*TimeSeries)
}

func (p *TimeSeriesSlicePool) Put(v *[]*TimeSeries) {
	*v = (*v)[:0]
	p.p.Put(v)
}

type BufferPool struct {
	p sync.Pool
}

func (p *BufferPool) Get() *bytes.Buffer {
	v := p.p.Get()
	if v == nil {
		return bytes.NewBuffer(nil)
	}
	return v.(*bytes.Buffer)
}

func (p *BufferPool) Put(v *bytes.Buffer) {
	v.Reset()
	p.p.Put(v)
}

type InterfaceSlicePool struct {
	p sync.Pool
}

func (isp *InterfaceSlicePool) Get() *[]interface{} {
	isv := isp.p.Get()
	if isv == nil {
		return &[]interface{}{}
	}
	return isv.(*[]interface{})
}

func (isp *InterfaceSlicePool) Put(isv *[]interface{}) {
	*isv = (*isv)[:0]
	isp.p.Put(isv)
}
