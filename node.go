package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
)

const (
	NodeEtcdPrefix = "/easyid/node/master/%d" // %d should be format to `Node.Index`
)

type Node struct {
	Index int // 表示节点ID,注册到etcd中拼接在前缀中
	Name  string
	TTL   int64

	id         string
	protocol   int
	masterLock sync.RWMutex
	master     bool // 主节点标识
	meta       map[string]interface{}
	etcdCli    *clientv3.Client
	releaseCh  chan struct{}
	leaseID    clientv3.LeaseID
}

func NewNode(index int, name string, ttl int64, etcdCli *clientv3.Client) *Node {
	return &Node{
		Index:     index,
		Name:      name,
		TTL:       ttl,
		id:        UUID(),
		meta:      make(map[string]interface{}),
		releaseCh: make(chan struct{}),
		master:    false,
		etcdCli:   etcdCli,
	}
}

func (n *Node) Register(ctx context.Context) error {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				if n.GetMaster() {
					log.Printf("node:%s is master\n", n.id)
				}
			}
		}
	}()

	go func() {
		if err := n.campaign0(ctx); err != nil {
			log.Fatalf("节点{index:%d, id:%s} register err: %v\n", n.Index, n.id, err)
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
	n.leaseID = leaseID

	keepaliveRespChan, err := c.KeepAlive(ctx, leaseID)
	if err != nil {
		return err
	}

	keepaliveSuccessChan := make(chan struct{})

	var (
		key = fmt.Sprintf(NodeEtcdPrefix, n.Index)
		val = fmt.Sprintf("nodeId:%s,nodeName:%s,leaseId:%x", n.id, n.Name, leaseID)
	)

	var txnResp *clientv3.TxnResponse

	for {
		log.Printf("节点{index:%d, id:%s} 开始抢主\n", n.Index, n.id)
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

	log.Printf("节点{index:%d, id:%s} 抢主成功\n", n.Index, n.id)

	go func() {
	LOOP:
		for {
			select {
			case <-time.After(time.Duration(n.TTL-1) * time.Second):
				log.Printf("节点{index:%d, id:%s} keepalive 超时\n", n.Index, n.id)
				// 节点TTL即将过期，提前1秒设置失效状态，停止接收请求
				n.SetMaster(false)
				break LOOP
			case <-keepaliveSuccessChan: // 接收到 keepalive success 信号，重新开始计时
			case <-n.releaseCh:
				log.Printf("节点{index:%d, id:%s} 接收到 release 信号，退出 keepalive goroutine\n", n.Index, n.id)
				break LOOP
			}
		}
	}()

	for kaResp := range keepaliveRespChan {
		if kaResp != nil {
			log.Printf("节点{index:%d, id:%s} keepalive success leaseId{%d}\n", n.Index, n.id, kaResp.ID)
			keepaliveSuccessChan <- struct{}{}
		} else {
			log.Printf("节点{index:%d, id:%s} keepalive fail\n", n.Index, n.id)
		}
	}
	log.Printf("节点{index:%d, id:%s} keepalive chan closed\n", n.Index, n.id)

	return nil
}

func (n *Node) Deregister(ctx context.Context) error {
	panic("implement me")
}

func (n *Node) Watch(ctx context.Context, key string) error {
	// TODO 需要 watch 所有 master 节点，并在本地缓存所有 master 节点，后面需要按照节点 index hash 分组将请求打散到所有节点
	panic("implement me")
}

func (n *Node) Release(ctx context.Context) error {
	close(n.releaseCh)
	n.etcdCli.Lease.Revoke(ctx, n.leaseID)
	n.etcdCli.Close()
	return nil
}

func (n *Node) GetMaster() bool {
	var _master bool
	n.masterLock.RLock()
	_master = n.master
	n.masterLock.RUnlock()
	return _master
}

func (n *Node) SetMaster(master bool) {
	n.masterLock.Lock()
	n.master = master
	n.masterLock.Unlock()
}
