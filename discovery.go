package main

import "context"

type Discovery interface {
	Register(ctx context.Context) error
	Deregister(ctx context.Context) error
	Watch(ctx context.Context, key string) error
}
