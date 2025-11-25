package config

import (
	"sync"

	"github.com/995933447/discovery/manager"
	"github.com/995933447/easymicro/loader"
	easymicrolog "github.com/995933447/easymicro/log"
	"github.com/995933447/fastlog"
	"github.com/995933447/fastlog/logger"
)

const ServerConfigFileName = "healthcheckerserver"

type ServerConfig struct {
	SamplePProfTimeLongSec        int    `mapstructure:"sample_pprof_time_long_sec"`
	Env                           string `mapstructure:"env"`
	Discovery                     string `mapstructure:"discovery"`
	CheckWorkerPoolSize           uint32 `mapstructure:"check_worker_pool_size"`
	CheckIntervalMs               uint32 `mapstructure:"check_interval_ms"`
	HardDeleteNodeOverFailTimeSec int    `mapstructure:"hard_delete_node_over_fail_time_sec"`
	EnabledNats                   bool   `mapstructure:"enabled_nats"`
}

func (c *ServerConfig) GetDiscovery() string {
	if c.Discovery == "" {
		return manager.DefaultDiscoveryName
	}
	return c.Discovery
}

func (c *ServerConfig) IsDev() bool {
	return c.Env == "dev"
}

func (c *ServerConfig) IsTest() bool {
	return c.Env == "test"
}

func (c *ServerConfig) IsProd() bool {
	return c.Env == "prod"
}

var (
	serverConfig   ServerConfig
	serverConfigMu sync.RWMutex
)

func getServerConfig() *ServerConfig {
	return &serverConfig
}

func SafeReadServerConfig(fn func(c *ServerConfig)) {
	serverConfigMu.RLock()
	defer serverConfigMu.RUnlock()
	fn(getServerConfig())
}

func SafeWriteServerConfig(fn func(c *ServerConfig)) {
	serverConfigMu.Lock()
	defer serverConfigMu.Unlock()
	fn(getServerConfig())
}

func LoadConfig() error {
	var err error

	var (
		log       *logger.Logger
		cfgLoader *logger.ConfLoader
	)
	err = loader.LoadAndWatchLogConfigFromLocal(func(cfg *loader.LogConfig) {
		var err error

		if cfgLoader == nil {
			cfgLoader, err = logger.NewConfLoader("", 10, (*logger.LogConf)(cfg))
			if err != nil {
				panic(err)
			}
		}

		if log == nil {
			log, err = fastlog.InitFileLogger(cfgLoader.GetConf().File.DefaultLogDir, "healthchecker_easymicro", 5, cfgLoader)
			if err != nil {
				panic(err)
			}

			easymicrolog.SetLogger(log)
			
			if err = cfg.LoadToInitFastlog(nil); err != nil {
				panic(err)
			}
		}

		cfgLoader.SetDefaultLogConf((*logger.LogConf)(cfg))
	})
	if err != nil {
		return err
	}

	err = loader.LoadAndWatchConfig(ServerConfigFileName, &serverConfig, &serverConfigMu, nil)
	if err != nil {
		return err
	}

	if err = loader.LoadEtcdFromLocal(); err != nil {
		return err
	}

	if err = loader.LoadDiscoveryFromLocal(); err != nil {
		return err
	}

	var enabledNats bool
	SafeReadServerConfig(func(c *ServerConfig) {
		enabledNats = c.EnabledNats
	})
	if enabledNats {
		if err = loader.LoadNatsFromLocal(); err != nil {
			return err
		}
	}

	return nil
}
