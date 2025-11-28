package elect

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/995933447/autoelectv2"
	"github.com/995933447/autoelectv2/factory"
	"github.com/995933447/easymicro/etcd"
	"github.com/995933447/easymicro/log"
	"github.com/995933447/easymicro/node"
	"github.com/995933447/routeredis"
)

var (
	ErrElectAlreadyExists  = errors.New("election already exists")
	ErrElectNotInitialized = errors.New("election not initialized")
)

const (
	DriverNil   = ""
	DriverEtcd  = "etcd"
	DriverRedis = "redis"
)

type Options struct {
	Driver        string
	EtcdConnName  string
	RedisConnName string
}

var (
	elect   autoelectv2.AutoElection
	electMu sync.RWMutex
)

func MustGetElect() autoelectv2.AutoElection {
	if elect == nil {
		panic(ErrElectNotInitialized)
	}
	return elect
}

func InitElect(ctx context.Context, opts *Options) error {
	electMu.RLock()

	if elect != nil {
		electMu.RUnlock()
		return ErrElectAlreadyExists
	}

	electMu.RUnlock()

	electMu.Lock()
	defer electMu.Unlock()

	if elect != nil {
		return ErrElectAlreadyExists
	} else {
		nodeName := node.GetName()
		if nodeName == "" {
			return errors.New("please set node name")
		}

		switch opts.Driver {
		case DriverRedis:
			redisPool, err := routeredis.NewDynamicConnPool(opts.RedisConnName)
			if err != nil {
				return err
			}

			elect, err = factory.NewAutoElection(
				factory.ElectDriverDistribMuRedis,
				factory.NewDistribMuRedisCfg(redisPool, nodeName, fmt.Sprintf("%s:%d", node.GetHost(), node.GetPort())),
			)
			if err != nil {
				return err
			}
		case DriverEtcd:
			etcdCli, err := etcd.GetConn(opts.EtcdConnName)
			if err != nil {
				return err
			}

			elect, err = factory.NewAutoElection(
				factory.ElectDriverDistribMuEtcdv3,
				factory.NewDistribMuEtcdv3Cfg(nodeName, etcdCli, 5),
			)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("driver %s not support", opts.Driver)
		}
	}

	elect.LoopInElectV2(ctx, func(err error) {
		log.Error(err)
	})

	return nil
}

func IsMaster() (bool, error) {
	electMu.RLock()
	defer electMu.RUnlock()
	if elect == nil {
		return false, ErrElectNotInitialized
	}

	return elect.IsMaster(), nil
}
