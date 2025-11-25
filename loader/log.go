package loader

import (
	"errors"
	"strings"
	"sync"

	"github.com/995933447/easymicro/node"
	"github.com/995933447/fastlog"
	"github.com/995933447/fastlog/logger"
	"github.com/995933447/fastlog/logger/writer"
	"github.com/fsnotify/fsnotify"
)

var ErrFastlogAlreadyInitialized = errors.New("fastlog is already initialized")

var LogConfigFileName = "log"

var DisabledSplitFastlogByNodeName bool

var (
	globalLogConfigMu  sync.RWMutex
	defaultLogConfig   *LogConfig
	defaultLogConfigMu sync.RWMutex
)

type OnLoadLogConfigCallback struct {
	mu          sync.RWMutex
	onLoadFuncs []func(*LogConfig)
}

var onLoadLogConfigMap sync.Map
var logConfigMuMap sync.Map

type LogConfig logger.LogConf

func (c *LogConfig) SetConfigMu(mu *sync.RWMutex) {
	logConfigMuMap.Store(c, mu)
}

func (c *LogConfig) GetConfigMu() (*sync.RWMutex, bool) {
	mu, ok := logConfigMuMap.Load(c)
	if !ok {
		return nil, false
	}
	return mu.(*sync.RWMutex), true
}

func (c *LogConfig) GetConfigMuDefaultGlobal() *sync.RWMutex {
	mu, ok := c.GetConfigMu()
	if !ok {
		mu = &globalLogConfigMu
	}
	return mu
}

func (c *LogConfig) AddOnLoadFunc(fn func(*LogConfig)) {
	cbAny, loaded := onLoadLogConfigMap.LoadOrStore(c, &OnLoadLogConfigCallback{
		onLoadFuncs: []func(*LogConfig){fn},
	})
	if loaded {
		cb := cbAny.(*OnLoadLogConfigCallback)
		cb.mu.Lock()
		defer cb.mu.Unlock()
		cb.onLoadFuncs = append(cb.onLoadFuncs, fn)
	}
}

func (c *LogConfig) OnLoad() {
	cbAny, ok := onLoadLogConfigMap.Load(c)
	if !ok {
		return
	}

	cb := cbAny.(*OnLoadLogConfigCallback)

	cb.mu.Lock()
	defer cb.mu.Unlock()

	mu := c.GetConfigMuDefaultGlobal()
	mu.Lock()
	cp := c.Copy()
	mu.Unlock()

	for _, fn := range cb.onLoadFuncs {
		fn(cp)
	}
}

func (c *LogConfig) SplitFastlogByNodeName() {
	if DisabledSplitFastlogByNodeName {
		return
	}

	nodeName := node.GetName()
	if nodeName == "" {
		return
	}

	dirNodeNameComp := "/" + nodeName

	if !strings.HasSuffix(c.File.DefaultLogDir, dirNodeNameComp) {
		c.File.DefaultLogDir = strings.TrimSuffix(c.File.DefaultLogDir, "/") + dirNodeNameComp
	}

	if !strings.HasSuffix(c.File.ExceptionLogDir, dirNodeNameComp) {
		c.File.ExceptionLogDir = strings.TrimSuffix(c.File.ExceptionLogDir, "/") + dirNodeNameComp
	}

	if !strings.HasSuffix(c.File.BillLogDir, dirNodeNameComp) {
		c.File.BillLogDir = strings.TrimSuffix(c.File.BillLogDir, "/") + dirNodeNameComp
	}

	if !strings.HasSuffix(c.File.StatLogDir, dirNodeNameComp) {
		c.File.StatLogDir = strings.TrimSuffix(c.File.StatLogDir, "/") + dirNodeNameComp
	}
}

func (c *LogConfig) Copy() *LogConfig {
	var cp LogConfig
	cp.File = c.File
	cp.AlertLevel = c.AlertLevel
	return &cp
}

var (
	defaultFastlogLoggerMu sync.RWMutex
)

func (c *LogConfig) LoadToInitFastlog(alertFunc writer.AlertFunc) error {
	defaultFastlogLoggerMu.RLock()
	_, ok := fastlog.GetDefaultCfgLoader()
	defaultFastlogLoggerMu.RUnlock()
	if ok {
		return ErrFastlogAlreadyInitialized
	}

	defaultFastlogLoggerMu.Lock()
	defer defaultFastlogLoggerMu.Unlock()

	_, ok = fastlog.GetDefaultCfgLoader()
	if ok {
		return ErrFastlogAlreadyInitialized
	}

	if err := fastlog.InitDefaultCfgLoader("", (*logger.LogConf)(c)); err != nil {
		return err
	}

	name := node.GetName()
	if name == "" {
		name = "easymicro"
	}

	fastlog.SetModuleName(name)

	if err := fastlog.InitDefaultLogger(alertFunc); err != nil {
		return err
	}

	fastlog.InitDefaultMsgStat(name)

	return nil
}

func LoadFastlogFromLocal(alertFunc writer.AlertFunc) error {
	cfgLoader, ok := fastlog.GetDefaultCfgLoader()
	if !ok {
		err := LoadAndWatchLogConfigFromLocal(func(cfg *LogConfig) {
			cfgLoader, ok = fastlog.GetDefaultCfgLoader()
			if !ok {
				if err := cfg.LoadToInitFastlog(alertFunc); err != nil {
					panic(err)
				}
				return
			}

			cfgLoader.SetDefaultLogConf((*logger.LogConf)(cfg))
		})
		if err != nil {
			return err
		}
		return nil
	}

	return ErrFastlogAlreadyInitialized
}

func LoadAndWatchLogConfigFromLocal(onLoad func(cfg *LogConfig)) error {
	defaultLogConfigMu.RLock()
	if defaultLogConfig != nil {
		if onLoad == nil {
			defaultLogConfigMu.RUnlock()
			return nil
		}
		defaultLogConfig.AddOnLoadFunc(onLoad)
		defaultLogConfigMu.RUnlock()
		defaultLogConfig.OnLoad()
		return nil
	}
	defaultLogConfigMu.RUnlock()

	defaultLogConfigMu.Lock()
	if defaultLogConfig != nil {
		if onLoad == nil {
			defaultLogConfigMu.RUnlock()
			return nil
		}
		defaultLogConfig.AddOnLoadFunc(onLoad)
		defaultLogConfigMu.Unlock()
		defaultLogConfig.OnLoad()
		return nil
	}

	err := unsafeLoadLogConfigFromLocal()
	defaultLogConfig.AddOnLoadFunc(onLoad)
	defaultLogConfigMu.Unlock()
	if err != nil {
		return err
	}

	defaultLogConfig.OnLoad()

	return nil
}

func unsafeLoadLogConfigFromLocal() error {
	viper, err := LoadConfigToViper(LogConfigFileName)
	if err != nil {
		return err
	}

	defaultLogConfig = &LogConfig{}
	if err = viper.Unmarshal(defaultLogConfig); err != nil {
		return err
	}

	defaultLogConfig.SplitFastlogByNodeName()
	defaultLogConfig.SetConfigMu(&defaultLogConfigMu)

	viper.OnConfigChange(func(in fsnotify.Event) {
		mu := defaultLogConfig.GetConfigMuDefaultGlobal()
		mu.Lock()
		old := defaultLogConfig.Copy()
		if err := viper.Unmarshal(defaultLogConfig); err != nil {
			defaultLogConfig.File = old.File
			defaultLogConfig.AlertLevel = old.AlertLevel
			mu.Unlock()
			return
		}

		defaultLogConfig.SplitFastlogByNodeName()
		mu.Unlock()

		defaultLogConfig.OnLoad()
	})

	viper.WatchConfig()

	// 避免启动watch期间，数据变了所以，再watch一次
	if err = viper.ReadInConfig(); err != nil {
		return err
	}

	if err = viper.Unmarshal(defaultLogConfig); err != nil {
		return err
	}

	defaultLogConfig.SplitFastlogByNodeName()

	return nil
}

func GetOrLoadAndWatchLogConfigFromLocal() (*LogConfig, error) {
	if defaultLogConfig == nil {
		if err := LoadAndWatchLogConfigFromLocal(nil); err != nil {
			return nil, err
		}
	}

	mu := defaultLogConfig.GetConfigMuDefaultGlobal()

	mu.RLock()
	defer mu.RUnlock()

	return defaultLogConfig.Copy(), nil
}
