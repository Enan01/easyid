package main

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
)

type Node struct {
	Index int // 表示节点ID,注册到etcd中拼接在前缀中
	Name  string
	TTL   int

	id       string
	ntype    NodeType
	protocol int
	master   bool
	meta     map[string]interface{}
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

func (n *Node) Deregister(ctx context.Context) error {
	panic("implement me")
}
func (n *Node) Watch(ctx context.Context, key string) error {
	panic("implement me")
}
