package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/Enan01/easyid/api"

	"google.golang.org/grpc"
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
		MaxId:  0,
	}

	var nextMaxId uint64
	// 获取当前用户最大ID
	resp, err := etcdCli.Get(ctx, fmt.Sprintf(UserIDKey, userId))
	if err != nil {
		log.Fatalf("[NewUserIDGenerator] err=%s", err)
	}
	if len(resp.Kvs) > 0 {
		curMaxId, _ := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
		log.Printf("user:%d maxId:%d", gen.UserId, curMaxId)
		gen.CurId = &AtomicUint{i: curMaxId}
		nextMaxId = curMaxId + IDInterval
	} else {
		nextMaxId = IDInterval
	}
	_, err = etcdCli.Put(ctx, fmt.Sprintf(UserIDKey, userId), strconv.FormatUint(nextMaxId, 10))
	if err != nil {
		log.Fatalf("[NewUserIDGenerator] err=%s", err)
	}
	return gen
}

// Next 获取下一个 ID
func (g *UserIDGenerator) Next(ctx context.Context) (uint64, error) {
	next := g.CurId.Incr()
	if g.MaxId-next <= 200 {
		log.Printf("触发分配下个 ID 区间")
		if err := g.AllocNextIDInterval(ctx); err != nil {
			return 0, err
		}
	}
	return next, nil
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

var LocalIDGenerators = &UserIDGenerators{}

type UserIDGenerators struct {
	gens sync.Map
}

func (gs *UserIDGenerators) GetByUserId(ctx context.Context, userId uint64) *UserIDGenerator {
	gen, ok := gs.gens.Load(userId)
	if ok {
		return gen.(*UserIDGenerator)
	}
	_gen := NewUserIDGenerator(ctx, userId)
	gs.gens.Store(userId, _gen)
	return _gen
}

// TODO 支持批量获取
func NextByUserIds(ctx context.Context, userIds []uint64) (map[uint64]uint64, error) {
	res := make(map[uint64]uint64, len(userIds))
	for _, uid := range userIds {
		idx := uid % 3
		if idx == uint64(nodeIndex) {
			id, err := LocalIDGenerators.GetByUserId(ctx, uid).Next(ctx)
			if err != nil {
				return nil, err
			}
			res[uid] = id
		} else {
			conn, err := grpc.Dial(fmt.Sprintf("%s:%s", AllMasterNodes[int(idx)].IP, AllMasterNodes[int(idx)].Port), grpc.WithInsecure())
			if err != nil {
				return nil, err
			}
			defer conn.Close()
			c := api.NewIdGeneratorClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r, err := c.Next(ctx, &api.UserId{UserId: uid})
			if err != nil {
				return nil, err
			}
			res[uid] = r.GetId()
		}
	}
	return res, nil
}
