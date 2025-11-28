package cache

import (
	"github.com/995933447/fastlog"
)

func Init() error {
	if err := JobGroupCacheManager.Init(); err != nil {
		return err
	}
	if err := InitSyncCacheToDBSched(); err != nil {
		fastlog.Errorf("err:%v", err)
		return err
	}
	return nil
}
