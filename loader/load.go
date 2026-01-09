package loader

import (
	"os"
	"sync"

	"github.com/995933447/easymicro/log"
	"github.com/995933447/easymicro/node"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

var (
	OsEnvVarKeyConfigDirPath  = "EASYMICRO_CONFIG_DIR"
	PrivateConfigBasicDirName = "easymicro_loader"
)

func GetSearchConfDirPaths() []string {
	var dirs []string

	dirs = append(dirs, "."+string(os.PathSeparator)+PrivateConfigBasicDirName+string(os.PathSeparator))

	nodeName := node.GetName()

	if nodeName != "" {
		dirs = append(dirs, ".."+string(os.PathSeparator)+PrivateConfigBasicDirName+string(os.PathSeparator)+nodeName)
	}
	dirs = append(dirs, ".."+string(os.PathSeparator)+PrivateConfigBasicDirName)

	if nodeName != "" {
		dirs = append(dirs, ".."+string(os.PathSeparator)+".."+string(os.PathSeparator)+PrivateConfigBasicDirName+string(os.PathSeparator)+nodeName)
	}
	dirs = append(dirs, ".."+string(os.PathSeparator)+".."+string(os.PathSeparator)+PrivateConfigBasicDirName)

	osVarPath := os.Getenv(OsEnvVarKeyConfigDirPath)
	if osVarPath != "" {
		if nodeName != "" {
			dirs = append(dirs, osVarPath+string(os.PathSeparator)+nodeName)
		}
		dirs = append(dirs, osVarPath)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "$HOME"
	}

	if nodeName != "" {
		dirs = append(dirs, homeDir+string(os.PathSeparator)+PrivateConfigBasicDirName+string(os.PathSeparator)+nodeName)
	}
	dirs = append(dirs, homeDir+string(os.PathSeparator)+PrivateConfigBasicDirName)

	dirs = append(dirs, DefaultConfigDirPath)

	return dirs
}

func LoadConfigToViper(configFileName string) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigName(configFileName)

	for _, dir := range GetSearchConfDirPaths() {
		v.AddConfigPath(dir)
	}

	err := v.ReadInConfig()
	if err != nil {
		return nil, err
	}

	return v, nil
}

func LoadAndWatchConfig(configFileName string, data any, dataMu sync.Locker, onWatch func(v *viper.Viper, in fsnotify.Event)) error {
	v, err := LoadConfigToViper(configFileName)
	if err != nil {
		return err
	}

	for _, dir := range GetSearchConfDirPaths() {
		v.AddConfigPath(dir)
	}

	err = v.ReadInConfig()
	if err != nil {
		return err
	}

	dataMu.Lock()
	if err = v.Unmarshal(data); err != nil {
		dataMu.Unlock()
		return err
	}
	dataMu.Unlock()

	v.OnConfigChange(func(in fsnotify.Event) {
		dataMu.Lock()
		if err = v.Unmarshal(data); err != nil {
			dataMu.Unlock()
			log.Error(err)
		}
		dataMu.Unlock()

		if onWatch != nil {
			onWatch(v, in)
		}
	})

	v.WatchConfig()

	// 避免还没watch配置就变了，再load一次
	err = v.ReadInConfig()
	if err != nil {
		return err
	}

	dataMu.Lock()
	defer dataMu.Unlock()
	if err = v.Unmarshal(data); err != nil {
		return err
	}

	return nil
}
