package remote

import (
	"sync"

	"github.com/prometheus/prometheus/prompb"
)

type QueryResultSlicePool struct {
	p sync.Pool
}

func (p *QueryResultSlicePool) Get() *[]*prompb.QueryResult {
	v := p.p.Get()
	if v == nil {
		return &[]*prompb.QueryResult{}
	}
	return v.(*[]*prompb.QueryResult)
}

func (p *QueryResultSlicePool) Put(v *[]*prompb.QueryResult) {
	*v = (*v)[:0]
	p.p.Put(v)
}
