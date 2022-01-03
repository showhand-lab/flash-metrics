package store

import "sync"

type ArgSlicePool struct {
	p sync.Pool
}

func (asp *ArgSlicePool) Get() *[]interface{} {
	asv := asp.p.Get()
	if asv == nil {
		return &[]interface{}{}
	}
	return asv.(*[]interface{})
}

func (asp *ArgSlicePool) Put(asv *[]interface{}) {
	*asv = (*asv)[:0]
	asp.p.Put(asv)
}
