package main

import (
	"context"
	"fmt"
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
	TTL   int

	id       string
	protocol int
	master   bool
	meta     map[string]interface{}

	// TODO 需要有一个节点失效的状态，状态失效需要比 lease 过期早
}

func NewNode(index int, name string, ttl int) *Node {
	return &Node{
		Index: index,
		Name:  name,
		TTL:   ttl,
		id:    UUID(),
		meta:  make(map[string]interface{}),
	}
}

func (n *Node) Register(ctx context.Context) error {
	go n.campaign(etcdCli)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				if n.master {
					fmt.Printf("node:%s is master\n", n.id)
				}
			}
		}
	}()
	return nil
}

func (n *Node) campaign(c *clientv3.Client) {
	var (
		ttl    = n.TTL
		prefix = fmt.Sprintf("/easyid/node/master/%d", n.Index)
		val    = fmt.Sprintf("%s:%s", n.id, n.Name)
	)

	for {
		s, err := concurrency.NewSession(c, concurrency.WithTTL(ttl))
		if err != nil {
			fmt.Println("NewSession err: ", err)
			continue
		}
		e := concurrency.NewElection(s, prefix)

		if err = e.Campaign(context.Background(), val); err != nil {
			fmt.Println("Campaign err: ", err)
			continue
		}

		n.master = true

		select {
		case <-s.Done():
			n.master = false
		}
	}
}

func (n *Node) compaign0(ctx context.Context, c *clientv3.Client) (err error) {
	leaseResp, err := c.Lease.Grant(ctx, int64(n.TTL))
	if err != nil {
		return err
	}
	leaseID := leaseResp.ID

	var (
		key = fmt.Sprintf(NodeEtcdPrefix, n.Index)
		val = fmt.Sprintf("nodeId:%s,nodeName%s,leaseId:%x", n.id, n.Name, leaseID)
	)

	// create lease

	txn := c.Txn(ctx)
	txn = txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, val, clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(key))

	resp, err := txn.Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return fmt.Errorf("txn commit fail")
	}

	// TODO
	return nil
}

/**
// Campaign puts a value as eligible for the election. It blocks until
// it is elected, an error occurs, or the context is cancelled.
func (e *Election) Campaign(ctx context.Context, val string) error {
	s := e.session
	client := e.session.Client()

	k := fmt.Sprintf("%s%x", e.keyPrefix, s.Lease())
	txn := client.Txn(ctx).If(v3.Compare(v3.CreateRevision(k), "=", 0))
	txn = txn.Then(v3.OpPut(k, val, v3.WithLease(s.Lease())))
	txn = txn.Else(v3.OpGet(k))
	resp, err := txn.Commit()
	if err != nil {
		return err
	}
	e.leaderKey, e.leaderRev, e.leaderSession = k, resp.Header.Revision, s
	if !resp.Succeeded {
		kv := resp.Responses[0].GetResponseRange().Kvs[0]
		e.leaderRev = kv.CreateRevision
		if string(kv.Value) != val {
			if err = e.Proclaim(ctx, val); err != nil {
				e.Resign(ctx)
				return err
			}
		}
	}

	_, err = waitDeletes(ctx, client, e.keyPrefix, e.leaderRev-1)
	if err != nil {
		// clean up in case of context cancel
		select {
		case <-ctx.Done():
			e.Resign(client.Ctx())
		default:
			e.leaderSession = nil
		}
		return err
	}
	e.hdr = resp.Header

	return nil
}

*/

func (n *Node) Deregister(ctx context.Context) error {
	panic("implement me")
}
func (n *Node) Watch(ctx context.Context, key string) error {
	// TODO 需要 watch 所有 master 节点，并在本地缓存所有 master 节点，后面需要按照节点 index hash 分组将请求打散到所有节点
	panic("implement me")
}
