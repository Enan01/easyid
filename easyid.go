package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/Enan01/easyid/api"
)

// TODO 从节点支持接收 http 请求，并通过 grpc 调用主节点获取id

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

func NextByUserIds(ctx context.Context, userIds []uint64) (map[uint64]uint64, error) {
	if ThisMasterNode == nil {
		masterOfThisNode := AllMasterNodes[ThisNode.Index]
		if masterOfThisNode != nil {
			log.Printf("从节点[%d]转发请求到主节点", ThisNode.Index)
			conn, err := ThisNode.GetGrpcClientConnByIndex(ctx, ThisNode.Index)
			if err != nil {
				return nil, err
			}
			c := api.NewIdGeneratorClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r, err := c.SlaveNextByUserIds(ctx, &api.UserIds{UserIds: userIds})
			if err != nil {
				return nil, err
			}
			ids := r.GetIds()
			return ids, nil
		}
		return nil, errors.New("this node has no master node")
	}

	// TODO 不是主节点将请求转发至主节点

	res := make(map[uint64]uint64, len(userIds))
	var (
		uidGroup0 []uint64
		uidGroup1 []uint64
		uidGroup2 []uint64
	)
	// 用户ID分组
	for _, uid := range userIds {
		idx := uid % 3
		switch idx {
		case 0:
			uidGroup0 = append(uidGroup0, uid)
		case 1:
			uidGroup1 = append(uidGroup1, uid)
		case 2:
			uidGroup2 = append(uidGroup2, uid)
		}
	}

	nextIds := func(ctx context.Context, ni int, userIds []uint64) (map[uint64]uint64, error) {
		ids := make(map[uint64]uint64)
		if ni == nodeIndex { // 本地获取
			log.Printf("节点[%d] 用户id[%v]本地获取", ni, userIds)
			for _, uid := range userIds {
				id, err := LocalIDGenerators.GetByUserId(ctx, uid).Next(ctx)
				if err != nil {
					return nil, err
				}
				ids[uid] = id
			}
		} else { // rpc获取
			log.Printf("节点[%d] 用户id[%v]rpc获取", ni, userIds)
			conn, err := ThisMasterNode.GetGrpcClientConnByIndex(ctx, ni)
			if err != nil {
				return nil, err
			}
			c := api.NewIdGeneratorClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r, err := c.NextByUserIds(ctx, &api.UserIds{UserIds: userIds})
			if err != nil {
				return nil, err
			}
			ids = r.GetIds()
		}
		return ids, nil
	}

	ids0, err := nextIds(ctx, 0, uidGroup0)
	if err != nil {
		return nil, err
	}
	ids1, err := nextIds(ctx, 1, uidGroup1)
	if err != nil {
		return nil, err
	}
	ids2, err := nextIds(ctx, 2, uidGroup2)
	if err != nil {
		return nil, err
	}

	for uid, id := range ids0 {
		res[uid] = id
	}
	for uid, id := range ids1 {
		res[uid] = id
	}
	for uid, id := range ids2 {
		res[uid] = id
	}
	return res, nil
}
