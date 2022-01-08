package batch

import (
	"sync"

	"github.com/hashicorp/golang-lru/simplelru"
)

type LRU struct {
	sync.Mutex
	Inner *simplelru.LRU
}

func NewLRU(size int) *LRU {
	c, _ := simplelru.NewLRU(size, nil)
	return &LRU{
		Inner: c,
	}
}
