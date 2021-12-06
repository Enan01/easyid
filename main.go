package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	nodeIndex int
	nodeName  string
)

func init() {
	flag.IntVar(&nodeIndex, "idx", 1, "node index")
	flag.StringVar(&nodeName, "name", "easyid-node", "node name")
}

func main() {
	flag.Parse()
	ctx := context.Background()
	node := NewNode(nodeIndex, nodeName, 30, "127.0.0.1", "9527", etcdCli)

	node.Register(ctx)
	go node.WatchMaster(ctx)

	userIdGen := NewUserIDGenerator(ctx, 10086)
	for i := 0; i < 2001; i++ {
		log.Printf("userIdGen[%d] next id: %d", userIdGen.UserId, userIdGen.Next(ctx))
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	node.Release(ctx)
}
