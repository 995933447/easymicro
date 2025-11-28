package config

import (
	"sync"

	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/loader"
	"github.com/995933447/routeredis"
)

const ServerConfigFileName = "jobschedserver"

type ServerConfig struct {
	SamplePProfTimeLongSec int    `mapstructure:"sample_pprof_time_long_sec"`
	Env                    string `mapstructure:"env"`
	MongoDb                string `mapstructure:"mongo_db"`
	MongoConn              string `mapstructure:"mongo_conn"`
	RedisConn              string `mapstructure:"redis_conn"`
	PausedJob              bool   `mapstructure:"paused_job"`
	DiscoveryName          string `mapstructure:"discovery_name"`
}

func (c *ServerConfig) GetDiscoveryName() string {
	if c.DiscoveryName == "" {
		return jobsched.EasymicroDiscoveryName
	}
	return c.DiscoveryName
}

func (c *ServerConfig) GetRedisConn() string {
	if c.RedisConn == "" {
		return routeredis.DefaultConnName
	}
	return c.RedisConn
}

func (c *ServerConfig) GetMongoConn() string {
	if c.MongoConn == "" {
		return jobsched.JobConnName
	}
	return c.MongoConn
}

func (c *ServerConfig) GetMongoDb() string {
	if c.MongoDb == "" {
		return jobsched.JobDbName
	}
	return c.MongoDb
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
	err = loader.LoadFastlogFromLocal(nil)
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

	if err = loader.LoadNatsFromLocal(); err != nil {
		return err
	}

	if err = loader.LoadAndWatchRedisFromLocal(); err != nil {
		return err
	}

	if err = loader.LoadAndWatchMongoFromLocal(); err != nil {
		return err
	}

	return nil
}
