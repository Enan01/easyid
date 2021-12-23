package main

import (
	"context"
	"log"

	"github.com/Enan01/easyid/api"
)

type GrpcServer struct{}

func (s *GrpcServer) Next(ctx context.Context, in *api.UserId) (*api.ID, error) {
	id, err := LocalIDGenerators.GetByUserId(ctx, in.GetUserId()).Next(ctx)
	if err != nil {
		return nil, err
	}
	return &api.ID{
		Id: id,
	}, nil
}

func (s *GrpcServer) NextByUserIds(ctx context.Context, in *api.UserIds) (*api.IDs, error) {
	log.Printf("GRPC 批量获取 用户ID[%v]", in.GetUserIds())
	// userIds 去重
	uids := make(map[uint64]struct{})
	for _, uid := range in.GetUserIds() {
		uids[uid] = struct{}{}
	}

	ids := &api.IDs{Ids: make(map[uint64]uint64)}
	for uid := range uids {
		id, err := LocalIDGenerators.GetByUserId(ctx, uid).Next(ctx)
		if err != nil {
			return nil, err
		}
		ids.Ids[uid] = id
	}

	return ids, nil
}
