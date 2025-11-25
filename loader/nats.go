package loader

import (
	"sync"
	"time"

	"github.com/995933447/easymicro/util"
	"github.com/995933447/natsevent"
	"golang.org/x/sync/errgroup"
)

var NatsConfFileName = "nats"

var (
	defaultNatsConfig   *NatsConfig
	defaultNatsConfigMu sync.RWMutex
)

type NatsConnConfig struct {
	User           string   `json:"user" mapstructure:"user"`
	Password       string   `json:"password" mapstructure:"password"`
	TimeoutMillSec int      `json:"timeout" mapstructure:"timeout"`
	Secure         bool     `json:"secure" mapstructure:"secure"`
	RootCa         string   `json:"root_ca" mapstructure:"root_ca"`
	Servers        []string `json:"servers" mapstructure:"servers"`
}

func (c *NatsConnConfig) Copy() *NatsConnConfig {
	var cp NatsConnConfig
	cp.Servers = make([]string, len(c.Servers))
	copy(cp.Servers, c.Servers)
	cp.User = c.User
	cp.Password = c.Password
	cp.TimeoutMillSec = c.TimeoutMillSec
	cp.RootCa = c.RootCa
	cp.Secure = c.Secure
	return &cp
}

type NatsConfig struct {
	Conns map[string]*NatsConnConfig `json:"connections" mapstructure:"connections"`
}

func (c *NatsConfig) Copy() *NatsConfig {
	var cp NatsConfig
	cp.Conns = make(map[string]*NatsConnConfig, len(c.Conns))
	for k, v := range c.Conns {
		cp.Conns[k] = v.Copy()
	}
	return &cp
}

func (c *NatsConfig) LoadToConnect() error {
	var wg errgroup.Group
	for k, conn := range c.Conns {
		cc := &natsevent.ConnConfig{
			User:     conn.User,
			Password: conn.Password,
			Timeout:  time.Duration(conn.TimeoutMillSec) * time.Millisecond,
			Secure:   conn.Secure,
			RootCa:   conn.RootCa,
			Servers:  conn.Servers,
		}
		name := k
		wg.Go(func() error {
			if _, err := natsevent.Connect(name, cc); err != nil {
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

func LoadNatsConfigFromLocal() error {
	viper, err := LoadConfigToViper(NatsConfFileName)
	if err != nil {
		return err
	}

	defaultNatsConfigMu.Lock()
	defer defaultNatsConfigMu.Unlock()

	defaultNatsConfig = &NatsConfig{}
	err = viper.Unmarshal(defaultNatsConfig)
	if err != nil {
		return err
	}

	TryDecryptNatsConfig(defaultNatsConfig)
	
	return nil
}

func GetOrLoadNatsConfigFromLocal() (*NatsConfig, error) {
	if defaultNatsConfig == nil {
		if err := LoadNatsConfigFromLocal(); err != nil {
			return nil, err
		}
	}

	defaultNatsConfigMu.RLock()
	defer defaultNatsConfigMu.RUnlock()

	return defaultNatsConfig.Copy(), nil
}

func LoadNatsFromLocal() error {
	cfg, err := GetOrLoadNatsConfigFromLocal()
	if err != nil {
		return err
	}

	return cfg.LoadToConnect()
}

func TryDecryptNatsConfig(cfg *NatsConfig) {
	for _, conn := range cfg.Conns {
		if conn.User != "" {
			decrypted, ok, _ := util.Decrypt(conn.User)
			if ok {
				conn.User = decrypted
			}
		}

		if conn.Password != "" {
			decrypted, ok, _ := util.Decrypt(conn.Password)
			if ok {
				conn.Password = decrypted
			}
		}
	}
}
