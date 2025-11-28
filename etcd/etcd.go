package etcd

import (
	"errors"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrConnNotFound = errors.New("etcd conn does not found")

var etcdConns sync.Map

func SetConn(name string, conn *clientv3.Client) {
	oldConn, ok := etcdConns.Load(name)
	if ok {
		go func() {
			time.Sleep(time.Second * 5)
			_ = oldConn.(*clientv3.Client).Close()
		}()
	}

	etcdConns.Store(name, conn)
}

func GetConn(name string) (*clientv3.Client, error) {
	conn, ok := etcdConns.Load(name)
	if !ok {
		return nil, ErrConnNotFound
	}
	
	return conn.(*clientv3.Client), nil
}
