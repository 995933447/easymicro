package boot

import (
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/config"
	easymicrorouteredis "github.com/995933447/easymicro/routeredis"
	"github.com/995933447/routeredis"
)

func InitRouteredis() {
	routeredis.OnCmdDone = easymicrorouteredis.FastlogRedisCmd
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		routeredis.RegisterDefaultConnKeyRoute(c.GetRedisConn())
	})
}
