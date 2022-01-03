package store

import "sync"

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
