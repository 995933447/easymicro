package loader

import (
	"sync"

	"github.com/995933447/easymicro/log"
	"github.com/995933447/easymicro/util"
	"github.com/995933447/routeredis"
	"github.com/fsnotify/fsnotify"
	"golang.org/x/sync/errgroup"
)

const RedisConfFileName = "redis"

type RedisConnConfig struct {
	IdleCount              int      `json:"idle_count" mapstructure:"idle_count"` // 最大空闲连接数
	IdleTimeoutMillSec     int      `json:"idle_timeout_mill_sec" mapstructure:"idle_timeout_mill_sec"`
	MaxConnLifetimeMillSec int      `json:"max_conn_lifetime_mill_sec" mapstructure:"max_conn_lifetime_mill_sec"`
	MaxConnPoolSize        int      `json:"max_conn_pool_size" mapstructure:"max_conn_pool_size"` // 最大链接数量
	Servers                []string `json:"servers" mapstructure:"servers"`                       // 非分片集群模式下只能配置一个server
	Password               string   `json:"password" mapstructure:"password"`
	EnabledCluster         bool     `json:"enabled_cluster" mapstructure:"enabled_cluster"`
}

func (c *RedisConnConfig) Copy() *RedisConnConfig {
	var cp RedisConnConfig
	cp.EnabledCluster = c.EnabledCluster
	cp.Servers = make([]string, len(c.Servers))
	copy(cp.Servers, c.Servers)
	cp.Password = c.Password
	cp.MaxConnPoolSize = c.MaxConnPoolSize
	cp.IdleCount = c.IdleCount
	cp.IdleTimeoutMillSec = c.IdleTimeoutMillSec
	cp.MaxConnLifetimeMillSec = c.MaxConnLifetimeMillSec
	return &cp
}

type RedisConfig struct {
	Conns map[string]*RedisConnConfig `json:"connections" mapstructure:"connections"`
}

func (c *RedisConfig) Copy() *RedisConfig {
	var cp RedisConfig
	cp.Conns = make(map[string]*RedisConnConfig, len(c.Conns))
	for k, v := range c.Conns {
		cp.Conns[k] = v.Copy()
	}
	return &cp
}

func (c *RedisConfig) LoadToConnect() error {
	var wg errgroup.Group
	for k, conn := range c.Conns {
		cc := &routeredis.ConnConf{
			IdleCount:              conn.IdleCount,
			IdleTimeoutMillSec:     conn.IdleTimeoutMillSec,
			MaxConnLifetimeMillSec: conn.MaxConnLifetimeMillSec,
			MaxConnPoolSize:        conn.MaxConnPoolSize,
			Servers:                conn.Servers,
			Password:               conn.Password,
			EnabledCluster:         conn.EnabledCluster,
		}
		name := k
		wg.Go(func() error {
			if err := routeredis.ConnectByConf(name, cc); err != nil {
				return err
			}
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return err
	}

	return nil
}

var (
	defaultRedisConfig   *RedisConfig
	defaultRedisConfigMu sync.RWMutex
)

func LoadRedisConfigFromLocal() error {
	viper, err := LoadConfigToViper(RedisConfFileName)
	if err != nil {
		return err
	}

	defaultRedisConfigMu.Lock()
	defer defaultRedisConfigMu.Unlock()

	defaultRedisConfig = &RedisConfig{}
	err = viper.Unmarshal(defaultRedisConfig)
	if err != nil {
		return err
	}

	TryDecryptRedisConfig(defaultRedisConfig)

	return nil
}

func LoadRedisFromLocal() error {
	cfg, err := GetOrLoadRedisConfigFromLocal()
	if err != nil {
		return err
	}

	return cfg.LoadToConnect()
}

func GetOrLoadRedisConfigFromLocal() (*RedisConfig, error) {
	if defaultRedisConfig == nil {
		if err := LoadRedisConfigFromLocal(); err != nil {
			return nil, err
		}
	}

	defaultRedisConfigMu.RLock()
	defer defaultRedisConfigMu.RUnlock()

	return defaultRedisConfig.Copy(), nil
}

func ReloadRedisFromLocal() error {
	if err := LoadRedisConfigFromLocal(); err != nil {
		return err
	}

	defaultRedisConfigMu.RLock()
	cfg := defaultRedisConfig.Copy()
	defaultRedisConfigMu.RUnlock()

	return cfg.LoadToConnect()
}

func LoadAndWatchRedisFromLocal() error {
	viper, err := LoadConfigToViper(RedisConfFileName)
	if err != nil {
		return err
	}

	defaultRedisConfigMu.Lock()
	if defaultRedisConfig == nil {
		defaultRedisConfig = &RedisConfig{}
	}
	err = viper.Unmarshal(defaultRedisConfig)
	TryDecryptNatsConfig(defaultNatsConfig)
	cfg := defaultRedisConfig.Copy()
	defaultRedisConfigMu.Unlock()
	if err != nil {
		return err
	}

	viper.OnConfigChange(func(in fsnotify.Event) {
		defaultRedisConfigMu.Lock()
		if defaultRedisConfig == nil {
			defaultRedisConfig = &RedisConfig{}
		}
		err = viper.Unmarshal(defaultRedisConfig)
		TryDecryptRedisConfig(defaultRedisConfig)
		cfg := defaultRedisConfig.Copy()
		defaultRedisConfigMu.Unlock()
		if err != nil {
			log.Error(err)
			return
		}
		if err = cfg.LoadToConnect(); err != nil {
			log.Error(err)
		}
	})

	viper.WatchConfig()

	return cfg.LoadToConnect()
}

func TryDecryptRedisConfig(cfg *RedisConfig) {
	for _, conn := range cfg.Conns {
		if conn.Password != "" {
			decrypted, err := util.Decrypt(conn.Password)
			if err == nil {
				conn.Password = decrypted
			}
		}
	}
}
