package main

import (
	"context"
	"log"
	"strings"

	"github.com/995933447/easymicro/example/echo"
	"github.com/995933447/easymicro/example/echoserver/boot"
	"github.com/995933447/easymicro/example/echoserver/config"
	"github.com/995933447/easymicro/example/echoserver/event"

	_ "github.com/995933447/easymicro/example/echoserver/jobpub/impl"
	"github.com/995933447/easymicro/grpc/middleservice/job"
	"github.com/995933447/easymicro/grpc/middleservice/jobexec"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"

	"github.com/995933447/easymicro/grpc/interceptor"
	ggrpc "google.golang.org/grpc"

	"github.com/995933447/discovery"
	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/runtimeutil"
)

func main() {
	if err := boot.InitNode("echo"); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := config.LoadConfig(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	boot.InitRouteredis()

	if err := boot.InitElect(context.TODO()); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := boot.InitMgorm(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := event.RegisterEventListeners(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		if !c.IsProd() {
			if err := boot.RegisterNatsRPCRoutes(); err != nil {
				log.Fatal(runtimeutil.NewStackErr(err))
			}
		}

		if err := jobsched.PrepareGRPC(echo.EasymicroDiscoveryName); err != nil {
			log.Fatal(runtimeutil.NewStackErr(err))
		}
	})

	if err := grpc.PrepareDiscoverGRPC(context.TODO(), echo.EasymicroGRPCSchema, echo.EasymicroDiscoveryName); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	boot.RegisterGRPCDialOpts()

	signal, err := boot.InitSignal()
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	stopCtx, stopCancel := context.WithCancel(context.Background())
	gracefulStopCtx, gracefulStopCancel := context.WithCancel(stopCtx)

	err = signal.AppendSignalCallbackByAlias(boot.SignalAliasStop, func() {
		gracefulStopCancel()
	})
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	err = signal.AppendSignalCallbackByAlias(boot.SignalAliasInterrupt, func() {
		stopCancel()
	})
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	err = grpc.ServeGRPC(context.TODO(), &grpc.ServeGRPCOptions{
		DiscoveryName:   echo.EasymicroDiscoveryName,
		ServiceNames:    boot.ServiceNames,
		StopCtx:         stopCtx,
		GracefulStopCtx: gracefulStopCtx,
		OnRunServer: func(server *ggrpc.Server, node *discovery.Node) {
			jobexec.RegisterJobExecServer(server, &job.JobExecutor{})

			signal.Start()
			boot.InitApp()

			log.Printf("up node %s:%d !\n", node.Host, node.Port)
			log.Printf(">>>>>>>>>>>>>>> run %s successfully ! <<<<<<<<<<<<<<<", strings.Join(boot.ServiceNames, ", "))
		},
		RegisterServiceServersFunc: boot.RegisterServiceServers,
		EnabledHealth:              true,
		GRPCServerOpts: []ggrpc.ServerOption{
			ggrpc.ChainUnaryInterceptor(
				interceptor.RecoveryServeRPCUnaryInterceptor,
				interceptor.TraceServeRPCUnaryInterceptor,
				interceptor.FastlogServeRPCUnaryInterceptor,
			),
			ggrpc.ChainStreamInterceptor(
				interceptor.RecoveryServeRPCStreamInterceptor,
				interceptor.TraceServeRPCStreamInterceptor,
				interceptor.FastlogServeRPCStreamInterceptor,
			),
		},
	})
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}
}
