package main

import (
	"context"
	"fmt"

	"github.com/coreos/etcd/clientv3"
)

type Node struct {
	Index int
	Name  string
	TTL   int

	id       string
	ntype    NodeType
	protocol int
	metadata map[string]interface{}
}

func NewNode(index int, name string, ttl int) *Node {
	return &Node{
		Index:    index,
		Name:     name,
		TTL:      ttl,
		id:       UUID(),
		metadata: make(map[string]interface{}),
	}
}

func (n *Node) Register(ctx context.Context) error {
	var (
		kv    = clientv3.NewKV(etcdCli)
		lease = clientv3.NewLease(etcdCli)
		key = fmt.Sprintf("format string", a ...interface{})
	)

	
	panic("implement me")
}
func (n *Node) Deregister(ctx context.Context) error {
	panic("implement me")
}
func (n *Node) Watch(ctx context.Context, key string) error {
	panic("implement me")
}

/**
type Discovery interface {
	Register(ctx context.Context) error
	Deregister(ctx context.Context) error
	Watch(ctx context.Context, key string) error
}


// 将服务注册到etcd上
func RegisterServiceToETCD(ServiceTarget string, value string) {
    dir = strings.TrimRight(ServiceTarget, "/") + "/"

    client, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{"localhost:2379"},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
    panic(err)
    }

    kv := clientv3.NewKV(client)
    lease := clientv3.NewLease(client)
    var curLeaseId clientv3.LeaseID = 0

    for {
        if curLeaseId == 0 {
            leaseResp, err := lease.Grant(context.TODO(), 10)
            if err != nil {
              panic(err)
            }

            key := ServiceTarget + fmt.Sprintf("%d", leaseResp.ID)
            if _, err := kv.Put(context.TODO(), key, value, clientv3.WithLease(leaseResp.ID)); err != nil {
                  panic(err)
            }
            curLeaseId = leaseResp.ID
        } else {
      // 续约租约，如果租约已经过期将curLeaseId复位到0重新走创建租约的逻辑
            if _, err := lease.KeepAliveOnce(context.TODO(), curLeaseId); err == rpctypes.ErrLeaseNotFound {
                curLeaseId = 0
                continue
            }
        }
        time.Sleep(time.Duration(1) * time.Second)
    }
}
*/
