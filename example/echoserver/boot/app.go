package boot

import (
	"context"
	"log"
	"time"

	"github.com/995933447/easymicro/example/echo"
	"github.com/995933447/easymicro/example/echoserver/jobpub"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
)

func InitApp() {
	if err := InitJobs(); err != nil {
		log.Fatal(err)
	}
}

func InitJobs() error {
	err := jobpub.PubEchoServiceBasicEchoJob(context.TODO(), &jobsched.PubJobReq{
		UniqKey:                        echo.EasymicroGRPCPbServiceNameEcho + ".BasicEcho",
		Name:                           echo.EasymicroGRPCPbServiceNameEcho + ".BasicEcho",
		AllowReRunCountCazFailPerSched: 600,
		TimeCronExpr:                   "*/5 * * * * * *", // 5秒钟一次
		Mode:                           int32(jobsched.SchedJobMode_SchedJobModeTimeCron),
	}, &echo.EchoReq{
		Echo: "hello world",
	})
	if err != nil {
		return err
	}

	err = jobpub.PubEchoServiceBasicEchoJob(context.TODO(), &jobsched.PubJobReq{
		UniqKey:    echo.EasymicroGRPCPbServiceNameEcho + ".BasicEcho.Once",
		Name:       echo.EasymicroGRPCPbServiceNameEcho + ".BasicEcho.Once",
		SpecTimeAt: time.Now().Add(time.Second * time.Duration(10)).Unix(),
		Mode:       int32(jobsched.SchedJobMode_SchedJobModeSpecTime),
	}, &echo.EchoReq{
		Echo:   "hello once",
		Remark: "BasicEcho.Once",
	})
	if err != nil {
		return err
	}

	err = jobpub.PubEchoServiceProxyEchoAsyncJob(context.TODO(), &jobsched.PubJobReq{
		UniqKey:                        echo.EasymicroGRPCPbServiceNameEcho + ".ProxyEcho",
		Name:                           echo.EasymicroGRPCPbServiceNameEcho + ".ProxyEcho",
		AllowReRunCountCazFailPerSched: 600,
		TimeIntervalSec:                15,
		Mode:                           int32(jobsched.SchedJobMode_SchedJobModeTimeIntervalSec),
	}, &echo.EchoReq{
		Echo:       "hello ProxyEcho",
		AuthorName: "echo server",
	})
	if err != nil {
		return err
	}

	return nil
}
