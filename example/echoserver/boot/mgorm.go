package boot

import (
	easymciromgorm "github.com/995933447/easymicro/mgorm"
	"github.com/995933447/fastlog"
	"github.com/995933447/routeredis"
	"time"

	"github.com/995933447/mgorm"
)

func InitMgorm() error {
	pool, err := routeredis.NewDynamicConnPool("default")
	if err != nil {
		return err
	}

	mgorm.DefaultCache = mgorm.NewRedisCache(pool, func(ttl int64, err error, cost time.Duration, cmd string, key string, args ...interface{}) {
		fastlog.Infof("mgorm redis cache cmd:%s %s, args:%+v, ttl:%d, err:%v, cost:%s", cmd, key, args, ttl, err, cost)
	})

	mgorm.OnQueryDone = easymciromgorm.FastlogMgormQuery

	return nil
}
