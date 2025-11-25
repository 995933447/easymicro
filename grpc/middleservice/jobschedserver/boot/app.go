package boot

import (
	"context"
	"log"
	"time"

	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/cache"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/config"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/jobpub"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/looptask"
	"github.com/995933447/runtimeutil"
)

func InitApp() {
	if err := cache.Init(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := looptask.InitJobSched(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	initExpiredJobsClearer()
}

func initExpiredJobsClearer() {
	go func() {
		time.Sleep(time.Minute)
		pubJobReq := &jobsched.PubJobReq{
			Name:         "easymicro.grpc.middleservice.jobsched.clearExpiredJobs",
			UniqKey:      "easymicro.grpc.middleservice.jobsched.clearExpiredJobs",
			TimeCronExpr: "0 0 1 * * * ?",
			Mode:         int32(jobsched.SchedJobMode_SchedJobModeTimeCron),
		}
		config.SafeReadServerConfig(func(c *config.ServerConfig) {
			if !c.IsProd() {
				pubJobReq.TimeIntervalSec = 60
				pubJobReq.Mode = int32(jobsched.SchedJobMode_SchedJobModeTimeIntervalSec)
			}
		})
		err := jobpub.PubJobSchedServiceClearExpiredJobsAsyncJob(context.TODO(), pubJobReq, &jobsched.ClearExpiredJobsReq{})
		if err != nil {
			log.Fatal(runtimeutil.NewStackErr(err))
		}
	}()
}
