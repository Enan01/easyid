package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
)

const IDInterval = 1000
const UserIDKey = "/easyid/userid/%d"

type UserIDGenerator struct {
	UserId uint64
	CurId  *AtomicUint
	MaxId  uint64

	allocMutex sync.Mutex
}

func NewUserIDGenerator(ctx context.Context, userId uint64) *UserIDGenerator {
	gen := &UserIDGenerator{
		UserId: userId,
		CurId:  &AtomicUint{},
		MaxId:  0 + IDInterval,
	}

	// 获取当前用户最大ID
	resp, err := etcdCli.Get(ctx, fmt.Sprintf(UserIDKey, userId))
	if err != nil {
		log.Fatalf("[NewUserIDGenerator] err=%s", err)
	}
	if len(resp.Kvs) > 0 {
		curMaxId, _ := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
		gen.MaxId = curMaxId
	} else {
		nextMaxId := gen.MaxId
		_, err := etcdCli.Put(ctx, fmt.Sprintf(UserIDKey, userId), strconv.FormatUint(nextMaxId, 10))
		if err != nil {
			log.Fatalf("[NewUserIDGenerator] err=%s", err)
		}
	}
	return gen
}

// Next 获取下一个 ID
func (g *UserIDGenerator) Next(ctx context.Context) uint64 {
	next := g.CurId.Incr()
	if g.MaxId-next <= 200 {
		log.Printf("触发分配下个 ID 区间")
		g.AllocNextIDInterval(ctx)
	}
	return next
}

// AllocNextIDInterval 分配下个 ID 区间
func (g *UserIDGenerator) AllocNextIDInterval(ctx context.Context) error {
	g.allocMutex.Lock()
	defer g.allocMutex.Unlock()
	maxId := g.MaxId
	userIdKey := fmt.Sprintf(UserIDKey, g.UserId)
	resp, err := etcdCli.Get(ctx, userIdKey)
	if err != nil {
		return err
	}
	curMaxId, _ := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
	if maxId == curMaxId {
		nextMaxId := maxId + IDInterval
		_, err := etcdCli.Put(ctx, userIdKey, strconv.FormatUint(nextMaxId, 10))
		if err != nil {
			return err
		}
		g.MaxId = nextMaxId
	}
	return nil
}
