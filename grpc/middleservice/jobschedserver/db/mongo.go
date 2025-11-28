package db

import (
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/config"
)

func NewJobModel() *jobsched.JobModel {
	mod := jobsched.NewJobModel()
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		mod.SetConn(c.GetMongoConn())
		mod.SetDb(c.GetMongoDb())
	})
	return mod
}

func NewJobGroupModel() *jobsched.JobGroupModel {
	mod := jobsched.NewJobGroupModel()
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		mod.SetConn(c.GetMongoConn())
		mod.SetDb(c.GetMongoDb())
	})
	return mod
}
