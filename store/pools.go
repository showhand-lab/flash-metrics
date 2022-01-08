package store

import (
	"sync"

	"github.com/showhand-lab/flash-metrics-storage/store/model"
)

type TimeSeriesPool struct {
	p sync.Pool
}

func (p *TimeSeriesPool) Get() *model.TimeSeries {
	v := p.p.Get()
	if v == nil {
		return &model.TimeSeries{}
	}
	return v.(*model.TimeSeries)
}

func (p *TimeSeriesPool) Put(v *model.TimeSeries) {
	v.Labels = v.Labels[:0]
	v.Samples = v.Samples[:0]
	p.p.Put(v)
}

type TimeSeriesSlicePool struct {
	p sync.Pool
}

func (p *TimeSeriesSlicePool) Get() *[]*model.TimeSeries {
	v := p.p.Get()
	if v == nil {
		return &[]*model.TimeSeries{}
	}
	return v.(*[]*model.TimeSeries)
}

func (p *TimeSeriesSlicePool) Put(v *[]*model.TimeSeries) {
	*v = (*v)[:0]
	p.p.Put(v)
}
