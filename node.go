package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

const (
	NodeEtcdPrefix = "/easyid/node/master/%d" // %d should be format to `Node.Index`
)

type Node struct {
	Index int // 表示节点ID,注册到etcd中拼接在前缀中
	Name  string
	TTL   int64

	id       string
	protocol int
	master   bool
	meta     map[string]interface{}

	// TODO 需要有一个节点失效的状态，状态失效需要比 lease 过期早
}

func NewNode(index int, name string, ttl int64) *Node {
	return &Node{
		Index: index,
		Name:  name,
		TTL:   ttl,
		id:    UUID(),
		meta:  make(map[string]interface{}),
	}
}

func (n *Node) Register(ctx context.Context) error {
	go n.campaign0(ctx, etcdCli)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				if n.master {
					log.Printf("node:%s is master\n", n.id)
				}
			}
		}
	}()
	return nil
}

func (n *Node) campaign(ctx context.Context, c *clientv3.Client) {
	var (
		ttl    = int(n.TTL)
		prefix = fmt.Sprintf("/easyid/node/master/%d", n.Index)
		val    = fmt.Sprintf("%s:%s", n.id, n.Name)
	)

	for {
		s, err := concurrency.NewSession(c, concurrency.WithTTL(ttl))
		if err != nil {
			log.Println("NewSession err: ", err)
			continue
		}
		e := concurrency.NewElection(s, prefix)

		if err = e.Campaign(context.Background(), val); err != nil {
			log.Println("Campaign err: ", err)
			continue
		}

		n.master = true

		select {
		case <-s.Done():
			n.master = false
		}
	}
}

func (n *Node) campaign0(ctx context.Context, c *clientv3.Client) (err error) {
	leaseClient := clientv3.NewLease(c)
	leaseResp, err := leaseClient.Grant(ctx, n.TTL)
	if err != nil {
		return err
	}
	leaseID := leaseResp.ID

	keepaliveRespChan, err := leaseClient.KeepAlive(ctx, leaseID)
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
		log.Println("开始抢主", n.Index, n.id)
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
				time.Sleep(time.Second)
				continue
			}
		} else {
			break
		}
	}

	// 说明抢到主节点
	log.Println("抢主成功", n.Index, n.id)
	go func() {
		for {
			select {
			case <-time.After(time.Duration(n.TTL-1) * time.Second):
				log.Println("节点失效，将 master 状态置为 false", n.Index, n.id)
				// 节点TTL即将过期，提前1秒设置失效状态，停止接收请求
				n.master = false // TODO atomic modify
				break
			case <-keepaliveSuccessChan:
				// 接收到 keepalive success 信号，重新开始计时
			}
		}
		// TODO 缺少退出信号
	}()

	for kaResp := range keepaliveRespChan {
		if kaResp != nil {
			log.Println("keepalive success: ", kaResp.ID)
			keepaliveSuccessChan <- struct{}{}
		} else {
			log.Println("keepalive fail")
		}
	}
	log.Println("keepalive chan closed")

	return nil
}

func (n *Node) Deregister(ctx context.Context) error {
	panic("implement me")
}
func (n *Node) Watch(ctx context.Context, key string) error {
	// TODO 需要 watch 所有 master 节点，并在本地缓存所有 master 节点，后面需要按照节点 index hash 分组将请求打散到所有节点
	panic("implement me")
}
