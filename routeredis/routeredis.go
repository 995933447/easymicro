package routeredis

import (
	"time"

	"github.com/995933447/fastlog"
	"github.com/995933447/routeredis"
)

var FastlogRedisCmd routeredis.OnCmdDoneFunc = func(execCmdWay string, ttl *routeredis.TTL, cost time.Duration, cmd string, key *routeredis.Key, err error, args ...any) {
	fastlog.Infof("%s by router:%s to exec:%s %s %+v, cost:%s, err:%v", execCmdWay, key.Route, cmd, key.Key, args, cost, err)
}
