package loader

import (
	"sync"
	"time"

	"github.com/995933447/easymicro/log"
	"github.com/995933447/easymicro/util"
	"github.com/995933447/mgorm"
	"github.com/fsnotify/fsnotify"
	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"golang.org/x/sync/errgroup"
)

var MongoConfigFileName = "mongo"

type MongoWriteConcern struct {
	W interface{} `json:"w" mapstructure:"w"`

	Journal bool `json:"journal" mapstructure:"journal"`

	WTimeoutMillSec int64 `json:"w_timeout_mill_sec" mapstructure:"w_timeout_mill_sec"`
}

type MongoReadConcern struct {
	Level string `json:"level" mapstructure:"level"`
}

type MongoConnConfig struct {
	Scheme          string   `json:"scheme" mapstructure:"scheme"`
	Hosts           []string `json:"hosts" mapstructure:"hosts"`
	Query           string   `json:"query" mapstructure:"query"`
	User            string   `json:"user" mapstructure:"user"`
	Password        string   `json:"password" mapstructure:"password"`
	MaxPoolSize     uint64   `json:"max_pool_size" mapstructure:"max_pool_size"`
	MinPoolSize     uint64   `json:"min_pool_size" mapstructure:"min_pool_size"`
	ConnIdleTimeSec int      `json:"conn_idle_time_sec" mapstructure:"conn_idle_time_sec"`

	WriteConcern   string `json:"write_concern" mapstructure:"write_concern"`
	ReadConcern    string `json:"read_concern" mapstructure:"read_concern"`
	ReadPreference string `json:"read_preference" mapstructure:"read_preference"`
}

func (c *MongoConnConfig) Copy() *MongoConnConfig {
	var cp MongoConnConfig
	cp.Hosts = make([]string, len(c.Hosts))
	copy(cp.Hosts, c.Hosts)
	cp.User = c.User
	cp.Password = c.Password
	cp.Scheme = c.Scheme
	cp.ConnIdleTimeSec = c.ConnIdleTimeSec
	cp.MaxPoolSize = c.MaxPoolSize
	cp.MinPoolSize = c.MinPoolSize
	cp.Query = c.Query
	return &cp
}

type MongoConfig struct {
	Conns map[string]*MongoConnConfig `json:"connections" mapstructure:"connections"`
}

func (c *MongoConfig) Copy() *MongoConfig {
	var cp MongoConfig
	cp.Conns = make(map[string]*MongoConnConfig, len(c.Conns))
	for k, v := range c.Conns {
		cp.Conns[k] = v.Copy()
	}
	return &cp
}

func (c *MongoConfig) LoadToConnect() error {
	var wg errgroup.Group
	for k, cc := range c.Conns {
		mc := &mgorm.ConnConfig{
			Scheme:          cc.Scheme,
			Hosts:           cc.Hosts,
			Query:           cc.Query,
			User:            cc.User,
			Password:        cc.Password,
			MaxPoolSize:     cc.MaxPoolSize,
			MinPoolSize:     cc.MinPoolSize,
			ConnIdleTimeSec: cc.ConnIdleTimeSec,
		}

		name := k

		var opts []mgorm.ApplyConnOptFunc

		if cc.WriteConcern != "" {
			switch cc.WriteConcern {
			case "majority": // 等待多数节点确认并落盘最安全,数据持久,性能最差
				opts = append(opts, mgorm.WithWriteConcern(writeconcern.Majority()))
			case "w1": // 等待 primary 写入内存 常用默认值 若 primary 崩溃可能丢数据
				opts = append(opts, mgorm.WithWriteConcern(writeconcern.W1()))
			case "nack": // 不等待任何确认 速度最快 数据可能丢失
				opts = append(opts, mgorm.WithWriteConcern(writeconcern.Unacknowledged()))
			case "journaled": // 等待写入落盘 journal 防止主机宕机数据丢失 稍慢
				opts = append(opts, mgorm.WithWriteConcern(writeconcern.Journaled()))
			default: // 自定义
				var concern MongoWriteConcern
				if err := jsoniter.UnmarshalFromString(cc.WriteConcern, &concern); err != nil {
					return err
				}

				opts = append(opts, mgorm.WithWriteConcern(&writeconcern.WriteConcern{
					W:        concern.W,
					Journal:  &concern.Journal,
					WTimeout: time.Duration(concern.WTimeoutMillSec) * time.Millisecond,
				}))
			}
		}

		if cc.ReadConcern != "" {
			switch cc.ReadConcern {
			case "local": // 默认模式，只读本地节点已提交的操作日志，从 Primary 上立即读取可能会读到「未复制完成」的写入，如果主节点宕机，新主节点可能不包含这次转账 → 数据被回滚
				opts = append(opts, mgorm.WithReadConcern(readconcern.Local()))
			case "majority": // 最终一致性：只有当写入被多数节点确认后才可读取，确保读取到的数据不会丢失。允许从任意节点读取，只读取那些 已被 majority 确认的 oplog entries，可能读到老数据。
				opts = append(opts, mgorm.WithReadConcern(readconcern.Majority()))
			case "linearizable": // 最强一致性：不仅要求数据被多数确认，还保证你读的一定是「全系统最新」。确保读到最新的、已 majority 确认的写。只能从 Primary 读取。若发现选举可能在进行中，直接报错（防止幻读）。
				opts = append(opts, mgorm.WithReadConcern(readconcern.Linearizable()))
			case "available": // 就算从节点还没同步，也尽快返回，最快单数据最不可靠
				opts = append(opts, mgorm.WithReadConcern(readconcern.Available()))
			case "snapshot": // 保证多个集合/分片的读操作在「同一个时间点视图」上执行，常用于事务
				opts = append(opts, mgorm.WithReadConcern(readconcern.Snapshot()))
			default: // 自定义
				var concern MongoReadConcern
				if err := jsoniter.UnmarshalFromString(cc.ReadConcern, &concern); err != nil {
					return err
				}

				opts = append(opts, mgorm.WithReadConcern(&readconcern.ReadConcern{
					Level: concern.Level,
				}))
			}
		}

		if cc.ReadPreference != "" {
			switch cc.ReadPreference {
			case "secondary_preferred": // 优先读从节点，从节点全挂了从主节点读
				opts = append(opts, mgorm.WithReadPreference(readpref.SecondaryPreferred()))
			case "secondary": // 只从从节点读，没有可用从节点直接报错
				opts = append(opts, mgorm.WithReadPreference(readpref.Secondary()))
			case "primary_preferred": // 优先读主节点，主节点挂了读从节点
				opts = append(opts, mgorm.WithReadPreference(readpref.PrimaryPreferred()))
			case "primary": // 只读主节点，主节点挂了，直接报错
				opts = append(opts, mgorm.WithReadPreference(readpref.Primary()))
			case "nearest": // 任意节点中延迟最小的,自动选择延迟最低的
				opts = append(opts, mgorm.WithReadPreference(readpref.Nearest()))
			}
		}

		wg.Go(func() error {
			if _, err := mgorm.Connect(name, mc, opts...); err != nil {
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
	defaultMongoConfig   *MongoConfig
	defaultMongoConfigMu sync.RWMutex
)

func LoadMongoConfigFromLocal() error {
	viper, err := LoadConfigToViper(MongoConfigFileName)
	if err != nil {
		return err
	}

	defaultMongoConfigMu.Lock()
	defer defaultMongoConfigMu.Unlock()

	defaultMongoConfig = &MongoConfig{}
	err = viper.Unmarshal(defaultMongoConfig)
	if err != nil {
		return err
	}

	TryDecryptMongoConfig(defaultMongoConfig)

	return nil
}

func GetOrLoadMongoConfigFromLocal() (*MongoConfig, error) {
	if defaultMongoConfig == nil {
		if err := LoadMongoConfigFromLocal(); err != nil {
			return nil, err
		}
	}

	defaultMongoConfigMu.RLock()
	defer defaultMongoConfigMu.RUnlock()

	return defaultMongoConfig.Copy(), nil
}

func LoadMongoFromLocal() error {
	cfg, err := GetOrLoadMongoConfigFromLocal()
	if err != nil {
		return err
	}

	return cfg.LoadToConnect()
}

func ReloadMongoFromLocal() error {
	if err := LoadMongoConfigFromLocal(); err != nil {
		return err
	}

	defaultMongoConfigMu.RLock()
	cfg := defaultMongoConfig.Copy()
	defaultMongoConfigMu.RUnlock()

	return cfg.LoadToConnect()
}

func LoadAndWatchMongoFromLocal() error {
	viper, err := LoadConfigToViper(MongoConfigFileName)
	if err != nil {
		return err
	}

	defaultMongoConfigMu.Lock()
	if defaultMongoConfig == nil {
		defaultMongoConfig = &MongoConfig{}
	}
	err = viper.Unmarshal(defaultMongoConfig)
	TryDecryptMongoConfig(defaultMongoConfig)
	cfg := defaultMongoConfig.Copy()
	defaultMongoConfigMu.Unlock()
	if err != nil {
		return err
	}

	viper.OnConfigChange(func(in fsnotify.Event) {
		defaultMongoConfigMu.Lock()
		if defaultMongoConfig == nil {
			defaultMongoConfig = &MongoConfig{}
		}
		err = viper.Unmarshal(defaultMongoConfig)
		TryDecryptMongoConfig(defaultMongoConfig)
		cfg := defaultMongoConfig.Copy()
		defaultMongoConfigMu.Unlock()
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

func TryDecryptMongoConfig(cfg *MongoConfig) {
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
