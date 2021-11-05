package main

import (
	"context"
	"flag"
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
	ctx, cancel := context.WithCancel(context.Background())
	node := NewNode(nodeIndex, nodeName, 30, etcdCli)

	node.Register(ctx)
	go node.WatchMaster(ctx)

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	cancel()

	node.Release(ctx)
}
