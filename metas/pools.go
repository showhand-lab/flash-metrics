package metas

import "sync"

type LabelPairSlicePool struct {
	p sync.Pool
}

func (psp *LabelPairSlicePool) Get() *[]LabelPair {
	psv := psp.p.Get()
	if psv == nil {
		return &[]LabelPair{}
	}
	return psv.(*[]LabelPair)
}

func (psp *LabelPairSlicePool) Put(psv *[]LabelPair) {
	*psv = (*psv)[:0]
	psp.p.Put(psv)
}

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
