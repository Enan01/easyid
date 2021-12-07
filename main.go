package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	jsoniter "github.com/json-iterator/go"

	"github.com/Enan01/easyid/api"

	"google.golang.org/grpc"
)

var (
	nodeIndex int
	nodeName  string
	port      string
	httpPort  string
)

func init() {
	flag.IntVar(&nodeIndex, "idx", 1, "node index")
	flag.StringVar(&nodeName, "name", "easyid-node", "node name")
	flag.StringVar(&port, "gport", "9527", "grpc server port")
	flag.StringVar(&httpPort, "hport", "8000", "http server port")
}

func main() {
	flag.Parse()
	ctx := context.Background()
	node := NewNode(nodeIndex, nodeName, 30, "127.0.0.1", port, etcdCli)

	node.Register(ctx)
	go node.WatchMaster(ctx)

	//userIdGen := NewUserIDGenerator(ctx, 10086)
	//for i := 0; i < 2001; i++ {
	//	log.Printf("userIdGen[%d] next id: %d", userIdGen.UserId, userIdGen.Next(ctx))
	//}

	go grpcServerStart(port)
	go httpServerStart()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	<-sigc

	node.Release(ctx)
}

func grpcServerStart(port string) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	api.RegisterIdGeneratorServer(grpcServer, &GrpcServer{})

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("grpc server start fail: %s", err)
	}
}

func httpServerStart() {
	http.HandleFunc("/id-gen", func(w http.ResponseWriter, r *http.Request) {
		vars := r.URL.Query()
		userIdStr := vars["userIds"][0]
		userStrIds := strings.Split(userIdStr, ",")

		var userIds []uint64
		for _, uid := range userStrIds {
			_uid, _ := strconv.ParseUint(uid, 10, 64)
			userIds = append(userIds, _uid)
		}
		res, err := NextByUserIds(context.Background(), userIds)
		if err != nil {
			log.Printf("/id-gen cause err: %s", err)
			w.WriteHeader(500)
			w.Write([]byte("fail"))
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		js, _ := jsoniter.MarshalToString(res)
		w.Write([]byte(js))
	})
	if err := http.ListenAndServe(":"+httpPort, nil); err != nil {
		log.Fatalf("http server start fail: %s", err)
	}
}
