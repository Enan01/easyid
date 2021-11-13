package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"

	"github.com/coreos/etcd/clientv3"
)

const (
	MasterNodeEtcdPrefix = "/easyid/node/master" // %d should be format to `Node.Index`
)

// TODO 抽象节点序列化信息
var AllMasterNodes = make(map[string]string)

// TODO 节点 value 需要包含 ip, port，转发请求的时候需要用到，每个 node 会维护其他 node 的 client 连接池

type Node struct {
	Index int // 表示节点ID,注册到etcd中拼接在前缀中
	Name  string
	TTL   int64

	id        string
	protocol  int
	master    *AtomicBool // 主节点标识
	meta      map[string]interface{}
	etcdCli   *clientv3.Client
	releaseCh chan struct{}
	leaseID   clientv3.LeaseID
}

func NewNode(index int, name string, ttl int64, etcdCli *clientv3.Client) *Node {
	return &Node{
		Index:     index,
		Name:      name,
		TTL:       ttl,
		id:        UUID(),
		meta:      make(map[string]interface{}),
		releaseCh: make(chan struct{}),
		master:    &AtomicBool{flag: 0},
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
		key = fmt.Sprintf(MasterNodeEtcdPrefix+"/%d", n.Index)
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

// TODO 如何能够获取到所有的变更事件？后一个启动的节点无法感知到之前的节点
func (n *Node) WatchMaster(ctx context.Context) error {
	getResp, err := n.etcdCli.Get(ctx, MasterNodeEtcdPrefix, clientv3.WithPrefix())
	if err != nil {
		log.Printf("err: %s", err)
		return err
	}
	if len(getResp.Kvs) != 0 {
		for _, kv := range getResp.Kvs {
			AllMasterNodes[string(kv.Key)] = string(kv.Value)
		}
		log.Printf("当前值：%v", AllMasterNodes)
	}

	watchStartRevision := getResp.Header.Revision + 1

	wch := n.etcdCli.Watch(ctx, MasterNodeEtcdPrefix, clientv3.WithPrefix(), clientv3.WithRev(watchStartRevision))
	for wresp := range wch {
		for _, ev := range wresp.Events {
			kbs, vbs := ev.Kv.Key, ev.Kv.Value
			switch ev.Type {
			case mvccpb.PUT:
				log.Printf("节点{index:%d, id:%s} 监听到节点{%s} PUT: %s", n.Index, n.id, string(kbs), string(vbs))
				AllMasterNodes[string(kbs)] = string(vbs)
			case mvccpb.DELETE:
				log.Printf("节点{index:%d, id:%s} 监听到节点{%s} DELETE", n.Index, n.id, string(kbs))
				delete(AllMasterNodes, string(kbs))
			}
			log.Printf("当前值：%v", AllMasterNodes)
		}
	}
	log.Printf("节点{index:%d, id:%s} 结束 watcher", n.Index, n.id)
	// TODO 需要 watch 所有 master 节点，并在本地缓存所有 master 节点，后面需要按照节点 index hash 分组将请求打散到所有节点
	return nil
}

func (n *Node) Release(ctx context.Context) error {
	close(n.releaseCh)
	n.etcdCli.Lease.Revoke(ctx, n.leaseID)
	n.etcdCli.Close()
	return nil
}

func (n *Node) GetMaster() bool {
	return n.master.Get()
}

func (n *Node) SetMaster(master bool) {
	n.master.Set(master)
}
