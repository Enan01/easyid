package main

import (
	"sync/atomic"

	"github.com/google/uuid"
)

func UUID() string {
	return uuid.New().String()
}

type AtomicBool struct{ flag int32 }

func (b *AtomicBool) Set(value bool) {
	var i int32 = 0
	if value {
		i = 1
	}
	atomic.StoreInt32(&(b.flag), i)
}

func (b *AtomicBool) Get() bool {
	if atomic.LoadInt32(&(b.flag)) != 0 {
		return true
	}
	return false
}
