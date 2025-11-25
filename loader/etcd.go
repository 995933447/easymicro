package loader

import (
	"sync"

	"github.com/995933447/easymicro/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"
)

const EtcdConfFileName = "etcd"

var (
	defaultEtcdConfig   *EtcdConfig
	defaultEtcdConfigMu sync.RWMutex
)

type EtcdConfig struct {
	Conns map[string]*EtcdConnConfig `json:"connections" mapstructure:"connections"`
}

type EtcdConnConfig struct {
	Endpoints []string `json:"endpoints" mapstructure:"endpoints"`
}

func (c *EtcdConnConfig) Copy() *EtcdConnConfig {
	var cp EtcdConnConfig
	cp.Endpoints = make([]string, len(c.Endpoints))
	copy(cp.Endpoints, c.Endpoints)
	return &cp
}

func (c *EtcdConfig) Copy() *EtcdConfig {
	var cp EtcdConfig
	cp.Conns = make(map[string]*EtcdConnConfig)
	for k, v := range c.Conns {
		cp.Conns[k] = v.Copy()
	}
	return &cp
}

func (c *EtcdConfig) LoadToConnect() error {
	var wg errgroup.Group
	for k, conn := range c.Conns {
		cc := conn
		name := k
		wg.Go(func() error {
			cli, err := clientv3.New(clientv3.Config{
				Endpoints: cc.Endpoints,
			})
			if err != nil {
				return err
			}

			etcd.SetConn(name, cli)

			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return err
	}

	return nil
}

func LoadEtcdConfigFromLocal() error {
	viper, err := LoadConfigToViper(EtcdConfFileName)
	if err != nil {
		return err
	}

	defaultEtcdConfigMu.Lock()
	defer defaultEtcdConfigMu.Unlock()

	defaultEtcdConfig = &EtcdConfig{}
	return viper.Unmarshal(defaultEtcdConfig)
}

func GetOrLoadEtcdConfigFromLocal() (*EtcdConfig, error) {
	if defaultEtcdConfig == nil {
		if err := LoadEtcdConfigFromLocal(); err != nil {
			return nil, err
		}
	}

	defaultEtcdConfigMu.RLock()
	defer defaultEtcdConfigMu.RUnlock()

	return defaultEtcdConfig.Copy(), nil
}

func LoadEtcdFromLocal() error {
	cfg, err := GetOrLoadEtcdConfigFromLocal()
	if err != nil {
		return err
	}

	if err = cfg.LoadToConnect(); err != nil {
		return err
	}

	return nil
}
