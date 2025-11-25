package handler

import (
	"context"
	"fmt"
	"time"

	"github.com/995933447/easymicro/example/echo"
	"github.com/995933447/easymicro/example/echoserver/jobpub"
	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"google.golang.org/grpc/metadata"
)

func (e *Echo) ProxyEcho(ctx context.Context, req *echo.EchoReq) (*echo.EchoResp, error) {
	m, _ := metadata.FromIncomingContext(ctx)
	fmt.Println(m)
	time.Sleep(time.Second * time.Duration(10))
	err := jobpub.PubEchoServiceBasicEchoJob(ctx, &jobsched.PubJobReq{
		Name:       echo.EasymicroGRPCPbServiceNameEcho + ".ProxyEcho.BasicEcho",
		Mode:       int32(jobsched.SchedJobMode_SchedJobModeSpecTime),
		SpecTimeAt: time.Now().Add(time.Second * time.Duration(10)).Unix(),
	}, &echo.EchoReq{
		Echo:       req.Echo,
		AuthorName: req.AuthorName + ".ProxyEcho",
	})
	if err != nil {
		return nil, err
	}
	return echo.EchoGRPC().BasicEcho(grpc.NewOutCtxFromInCtx(ctx), req)
}
