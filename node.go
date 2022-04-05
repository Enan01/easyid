package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/grpc"
)

const (
	MasterNodeEtcdPrefix = "/easyid/node/master" // %d should be format to `Node.Index`
)

var ThisNode *Node                       // 当前节点
var ThisMasterNode *Node                 // 当前 master 节点
var AllMasterNodes = make(map[int]*Node) // key: node index, val: node value json

type Node struct {
	ID    string `json:"id,omitempty"`
	Index int    `json:"index,omitempty"` // 表示节点ID,注册到etcd中拼接在前缀中
	Name  string `json:"name,omitempty"`
	TTL   int64  `json:"ttl,omitempty"`

	IP   string `json:"ip,omitempty"`
	Port string `json:"port,omitempty"`

	LeaseID clientv3.LeaseID `json:"leaseID,omitempty"`

	protocol  int                    `json:"-"`
	master    *AtomicBool            `json:"-"` // 主节点标识
	meta      map[string]interface{} `json:"-"`
	etcdCli   *clientv3.Client       `json:"-"`
	releaseCh chan struct{}          `json:"-"`

	nodeGrpcClientConns     map[int]*grpc.ClientConn `json:"-"` // 其他节点的grpc客户端连接 key 为 node index
	nodeGrpcClientConnsLock sync.RWMutex
}

func NewNode(index int, name string, ttl int64, ip string, port string, etcdCli *clientv3.Client) *Node {
	return &Node{
		Index:               index,
		Name:                name,
		TTL:                 ttl,
		IP:                  ip,
		Port:                port,
		ID:                  UUID(),
		meta:                make(map[string]interface{}),
		releaseCh:           make(chan struct{}),
		master:              &AtomicBool{flag: 0},
		etcdCli:             etcdCli,
		nodeGrpcClientConns: make(map[int]*grpc.ClientConn),
	}
}

func (n *Node) Register(ctx context.Context) error {
	ThisNode = n
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				if n.GetMaster() {
					log.Printf("node:%s is master\n", n.ID)
				}
			}
		}
	}()

	go func() {
		if err := n.campaign0(ctx); err != nil {
			log.Fatalf("节点{index:%d, id:%s} register err: %v\n", n.Index, n.ID, err)
		}
	}()
	return nil
}

func (n *Node) campaign0(ctx context.Context) (err error) {
	c := n.etcdCli
	leaseResp, err := c.Grant(ctx, n.TTL)
	if err != nil {
		return err
	}
	leaseID := leaseResp.ID
	n.LeaseID = leaseID

	keepaliveRespChan, err := c.KeepAlive(ctx, leaseID)
	if err != nil {
		return err
	}

	keepaliveSuccessChan := make(chan struct{})

	var (
		key    = fmt.Sprintf(MasterNodeEtcdPrefix+"/%d", n.Index)
		val, _ = jsoniter.MarshalToString(n)
	)

	var txnResp *clientv3.TxnResponse

	for {
		log.Printf("节点{index:%d, id:%s} 开始抢主\n", n.Index, n.ID)
		txn := c.Txn(ctx)
		txn = txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, val, clientv3.WithLease(leaseID))).
			Else(clientv3.OpGet(key))

		txnResp, err = txn.Commit()
		if err != nil {
			return err
		}
		if !txnResp.Succeeded {
			kv := txnResp.Responses[0].GetResponseRange().Kvs[0]
			if string(kv.Value) != val {
				// 不相等，说明当前节点已经不是主节点
				n.SetMaster(false)
				time.Sleep(time.Second)
				continue
			}
		} else {
			n.SetMaster(true)
			break
		}
	}

	log.Printf("节点{index:%d, id:%s} 抢主成功\n", n.Index, n.ID)
	ThisMasterNode = n

	go func() {
	LOOP:
		for {
			select {
			case <-time.After(time.Duration(n.TTL-1) * time.Second):
				log.Printf("节点{index:%d, id:%s} keepalive 超时\n", n.Index, n.ID)
				// 节点TTL即将过期，提前1秒设置失效状态，停止接收请求
				n.SetMaster(false)
				break LOOP
			case <-keepaliveSuccessChan: // 接收到 keepalive success 信号，重新开始计时
			case <-n.releaseCh:
				log.Printf("节点{index:%d, id:%s} 接收到 release 信号，退出 keepalive goroutine\n", n.Index, n.ID)
				break LOOP
			}
		}
	}()

	for kaResp := range keepaliveRespChan {
		if kaResp != nil {
			log.Printf("节点{index:%d, id:%s} keepalive success leaseId{%d}\n", n.Index, n.ID, kaResp.ID)
			keepaliveSuccessChan <- struct{}{}
		} else {
			log.Printf("节点{index:%d, id:%s} keepalive fail\n", n.Index, n.ID)
		}
	}
	log.Printf("节点{index:%d, id:%s} keepalive chan closed\n", n.Index, n.ID)

	return nil
}

func (n *Node) Deregister(ctx context.Context) error {
	panic("implement me")
}

func (n *Node) WatchMaster(ctx context.Context) error {
	getResp, err := n.etcdCli.Get(ctx, MasterNodeEtcdPrefix, clientv3.WithPrefix())
	if err != nil {
		log.Printf("err: %s", err)
		return err
	}
	if len(getResp.Kvs) != 0 {
		for _, kv := range getResp.Kvs {
			nodeIns := Node{}
			_ = jsoniter.Unmarshal(kv.Value, &nodeIns)
			AllMasterNodes[nodeIns.Index] = &nodeIns
		}
		log.Printf("当前值：%v", AllMasterNodes)
	}

	watchStartRevision := getResp.Header.Revision + 1

	// 如何能够获取到所有的变更事件？后一个启动的节点无法感知到之前的节点？ watch 的时候指定 `clientv3.WithPrefix()` `clientv3.WithRev`
	wch := n.etcdCli.Watch(ctx, MasterNodeEtcdPrefix, clientv3.WithPrefix(), clientv3.WithRev(watchStartRevision))
	for wresp := range wch {
		for _, ev := range wresp.Events {
			kbs, vbs := ev.Kv.Key, ev.Kv.Value
			switch ev.Type {
			case mvccpb.PUT:
				nodeIns := Node{}
				_ = jsoniter.Unmarshal(vbs, &nodeIns)
				log.Printf("节点{index:%d, id:%s} 监听到节点{%s} PUT: %s", n.Index, n.ID, string(kbs), string(vbs))
				AllMasterNodes[nodeIns.Index] = &nodeIns
				n.ReleaseGrpcClientConns(ctx)
			case mvccpb.DELETE:
				log.Printf("节点{index:%d, id:%s} 监听到节点{%s} DELETE", n.Index, n.ID, string(kbs))
				key := string(kbs)
				index, _ := strconv.Atoi(key[strings.LastIndex(key, "/")+1:])
				delete(AllMasterNodes, index)
				n.ReleaseGrpcClientConns(ctx)
			}
			log.Printf("当前值：%v", AllMasterNodes)
		}
	}
	log.Printf("节点{index:%d, id:%s} 结束 watcher", n.Index, n.ID)
	return nil
}

func (n *Node) Release(ctx context.Context) error {
	close(n.releaseCh)
	n.etcdCli.Lease.Revoke(ctx, n.LeaseID)
	n.etcdCli.Close()
	return nil
}

func (n *Node) GetMaster() bool {
	return n.master.Get()
}

func (n *Node) SetMaster(master bool) {
	n.master.Set(master)
}

// GetGrpcClientConnByIndex 根据index获取对应节点的 grpc 连接
func (n *Node) GetGrpcClientConnByIndex(ctx context.Context, idx int) (*grpc.ClientConn, error) {
	n.nodeGrpcClientConnsLock.RLock()
	conn := n.nodeGrpcClientConns[idx]
	n.nodeGrpcClientConnsLock.RUnlock()
	if conn != nil {
		return conn, nil
	}

	log.Printf("节点{index:%d, id:%s} 获取 grpc 连接，节点{index: %d} grpc 连接不存在触发创建", n.Index, n.ID, idx)
	n.nodeGrpcClientConnsLock.Lock()
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", AllMasterNodes[idx].IP, AllMasterNodes[idx].Port), grpc.WithInsecure())
	if err != nil {
		n.nodeGrpcClientConnsLock.Unlock()
		return nil, err
	}
	n.nodeGrpcClientConns[idx] = conn
	n.nodeGrpcClientConnsLock.Unlock()
	return conn, nil
}

// ReleaseGrpcClientConns 释放当前节点维护的所有其他节点的 grpc 连接
func (n *Node) ReleaseGrpcClientConns(ctx context.Context) {
	log.Printf("节点{index:%d, id:%s} 释放 grpc 连接", n.Index, n.ID)
	n.nodeGrpcClientConnsLock.RLock()
	grpcClientConns := n.nodeGrpcClientConns
	n.nodeGrpcClientConnsLock.RUnlock()

	n.nodeGrpcClientConnsLock.Lock()
	n.nodeGrpcClientConns = make(map[int]*grpc.ClientConn)
	n.nodeGrpcClientConnsLock.Unlock()

	for _, conn := range grpcClientConns {
		conn.Close()
	}
}
