package main

import (
	"context"

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
