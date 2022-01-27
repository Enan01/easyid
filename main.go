package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/Enan01/easyid/api"

	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
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

func idGenHandleFunc(ctx *fasthttp.RequestCtx) {
	userIdStr := string(ctx.URI().QueryArgs().Peek("userIds"))
	if userIdStr == "" {
		ctx.Error("param invalid", fasthttp.StatusBadRequest)
		return
	}

	userStrIds := strings.Split(userIdStr, ",")

	var userIds []uint64
	for _, uid := range userStrIds {
		_uid, err := strconv.ParseUint(uid, 10, 64)
		if err != nil {
			ctx.Error(fmt.Sprintf("param invalid: %s", err), fasthttp.StatusBadRequest)
			return
		}
		userIds = append(userIds, _uid)
	}

	res, err := NextByUserIds(context.Background(), userIds)
	if err != nil {
		log.Printf("id-gen cause err: %s", err)
		ctx.Error("id gen fail", fasthttp.StatusInternalServerError)
		return
	}

	body, _ := jsoniter.MarshalToString(res)
	ctx.Response.Header.Set("Content-Type", "application/json;charset=uft-8")
	ctx.Response.SetBodyString(body)
}

func httpServerStart() {
	m := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/id-gen":
			idGenHandleFunc(ctx)
		default:
			ctx.Error("not found", fasthttp.StatusNotFound)
		}
	}

	port := ":" + httpPort

	if err := fasthttp.ListenAndServe(port, m); err != nil {
		log.Fatalf("http server ListenAndServe fail: %s", err)
	}
}
