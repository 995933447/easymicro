package loader

import (
	"errors"
	"sync"
	"time"

	"github.com/995933447/discovery"
	"github.com/995933447/discovery/impl/debuglocalproxy"
	"github.com/995933447/discovery/impl/etcd"
	"github.com/995933447/discovery/impl/filecacheproxy"
	"github.com/995933447/discovery/manager"
	"github.com/995933447/easymicro/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var DiscoveryConfigFileName = "discovery"

type DiscoveryConfig struct {
	Discoveries map[string]*DiscoveryConfigDiscovery `json:"discoveries" mapstructure:"discoveries"`
}

type DiscoveryConfigDiscovery struct {
	DiscoverKeyPrefix              string `json:"discover_key_prefix" mapstructure:"discover_key_prefix"`
	Discovery                      string `json:"discovery" mapstructure:"discovery"`
	EtcdDiscoveryConfig            `json:"etcd" mapstructure:"etcd"`
	FileCacheProxyDiscoveryConfig  `json:"file_cache_proxy" mapstructure:"file_cache_proxy"`
	DebugLocalProxyDiscoveryConfig `json:"debug_local_proxy" mapstructure:"debug_local_proxy"`
}

type EtcdDiscoveryConfig struct {
	EtcdConn         string `json:"connection" mapstructure:"connection"`
	ConnectTimeoutMs int32  `json:"connect_timeout_ms" mapstructure:"connect_timeout_ms"`
}

type FileCacheProxyDiscoveryConfig struct {
	Dir      string `json:"dir" mapstructure:"dir"`
	ProxyFor string `json:"proxy_for" mapstructure:"proxy_for"`
}

type DebugLocalProxyDiscoveryConfig struct {
	Dir      string `json:"dir" mapstructure:"dir"`
	ProxyFor string `json:"proxy_for" mapstructure:"proxy_for"`
}

func (c *DiscoveryConfig) Copy() *DiscoveryConfig {
	var cp DiscoveryConfig
	cp.Discoveries = make(map[string]*DiscoveryConfigDiscovery)
	for name, v := range c.Discoveries {
		cp.Discoveries[name] = &DiscoveryConfigDiscovery{
			DiscoverKeyPrefix:              v.DiscoverKeyPrefix,
			Discovery:                      v.Discovery,
			EtcdDiscoveryConfig:            v.EtcdDiscoveryConfig,
			FileCacheProxyDiscoveryConfig:  v.FileCacheProxyDiscoveryConfig,
			DebugLocalProxyDiscoveryConfig: v.DebugLocalProxyDiscoveryConfig,
		}
	}
	return &cp
}

func (c *DiscoveryConfig) LoadToRegisterDiscovery() error {
	for name, v := range c.Discoveries {
		if v.Discovery == "" || v.Discovery == "customize" {
			return nil
		}

		dis, err := NewDiscovery(v.Discovery, v)
		if err != nil {
			return err
		}

		manager.RegisterDiscovery(name, dis)
	}

	return nil
}

var (
	defaultDiscoveryConfig   *DiscoveryConfig
	defaultDiscoveryConfigMu sync.RWMutex
)

func LoadDiscoveryConfigFromLocal() error {
	viper, err := LoadConfigToViper(DiscoveryConfigFileName)
	if err != nil {
		return err
	}

	defaultDiscoveryConfigMu.Lock()
	defer defaultDiscoveryConfigMu.Unlock()

	defaultDiscoveryConfig = &DiscoveryConfig{}
	return viper.Unmarshal(defaultDiscoveryConfig)
}

func GetOrDiscoveryConfigFromLocal() (*DiscoveryConfig, error) {
	if defaultDiscoveryConfig == nil {
		if err := LoadDiscoveryConfigFromLocal(); err != nil {
			return nil, err
		}
	}

	defaultDiscoveryConfigMu.RLock()
	defer defaultDiscoveryConfigMu.RUnlock()

	return defaultDiscoveryConfig.Copy(), nil
}

func LoadDiscoveryFromLocal() error {
	cfg, err := GetOrDiscoveryConfigFromLocal()
	if err != nil {
		return err
	}

	return cfg.LoadToRegisterDiscovery()
}

func NewDiscovery(discoveryTypeName string, cfg *DiscoveryConfigDiscovery) (discovery.Discovery, error) {
	switch discoveryTypeName {
	case "etcd":
		etcdCfg, err := GetOrLoadEtcdConfigFromLocal()
		if err != nil {
			return nil, err
		}

		etcdConnCfg, ok := etcdCfg.Conns[cfg.EtcdConn]
		if !ok {
			return nil, errors.New("etcd conn config not found")
		}

		return etcd.NewDiscovery(&etcd.Options{
			DiscoverKeyPrefix: cfg.DiscoverKeyPrefix,
			DiscoverTimeout:   time.Duration(cfg.EtcdDiscoveryConfig.ConnectTimeoutMs) * time.Millisecond,
			Etcd: &clientv3.Config{
				Endpoints: etcdConnCfg.Endpoints,
			},
			LogErrorFunc: func(err any) {
				log.Error(err)
			},
		})
	case "file_cache_proxy":
		proxyFor, err := NewDiscovery(cfg.FileCacheProxyDiscoveryConfig.ProxyFor, cfg)
		if err != nil {
			return nil, err
		}

		return filecacheproxy.NewDiscovery(&filecacheproxy.Options{
			Dir:       cfg.FileCacheProxyDiscoveryConfig.Dir,
			Discovery: proxyFor,
			LogErrFunc: func(err any) {
				log.Error(err)
			},
		})
	case "debug_local_proxy":
		proxyFor, err := NewDiscovery(cfg.DebugLocalProxyDiscoveryConfig.ProxyFor, cfg)
		if err != nil {
			return nil, err
		}

		dis := debuglocalproxy.NewDiscovery(&debuglocalproxy.Options{
			DiscoverLocalSrvDir: cfg.DebugLocalProxyDiscoveryConfig.Dir,
			Discovery:           proxyFor,
		})
		return dis, nil
	default:
	}

	return nil, errors.New("unknown discovery type: " + discoveryTypeName)
}
