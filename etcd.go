package main

import (
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
)

var (
	etcdCli *clientv3.Client
)

func init() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	etcdCli = cli
}

//                              ┌───────────────────────────────────────────────────────────────────┐
//                              │                                                                   │
//                              │                                                                   │
//                              │                                                                   │
//                              │                                                                   ▼
//                       ┌──────┴────────┐                ┌───────────────┐                  ┌───────────────┐
//                       │               │                │               │                  │               │
//                       │               │                │               │                  │               │
//                       │     node      ├───────────────►│     node      │                  │     node      │
//                       │               │                │               │                  │               │
//                       │               │                │               │                  │               │
//                       └───────────────┘                └───────────────┘                  └───────────────┘
//                              ▲
//                              │
//                              │
//                              │
//                              │
//                       ┌──────┴────────┐                ┌───────────────┐                  ┌───────────────┐
//                       │               │                │               │                  │               │
//                       │               │                │               │                  │               │
// request ─────────────►│     node      │                │     node      │                  │     node      │
//                       │               │                │               │                  │               │
//                       │               │                │               │                  │               │
//                       └───────────────┘                └───────────────┘                  └───────────────┘
