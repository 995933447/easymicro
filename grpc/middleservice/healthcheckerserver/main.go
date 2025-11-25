package main

import (
	"log"

	"github.com/995933447/discovery/manager"
	"github.com/995933447/easymicro/grpc/interceptor"
	"github.com/995933447/easymicro/grpc/middleservice/healthchecker"
	"github.com/995933447/easymicro/grpc/middleservice/healthcheckerserver/boot"
	"github.com/995933447/easymicro/grpc/middleservice/healthcheckerserver/config"
	"github.com/995933447/runtimeutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := boot.InitNode("healthchecker"); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	if err := config.LoadConfig(); err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	var checker *healthchecker.Checker
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		dis, err := manager.GetDiscovery(c.GetDiscovery())
		if err != nil {
			log.Fatal(err)
		}

		interceptors := []grpc.UnaryClientInterceptor{
			interceptor.TraceRPCUnaryInterceptor,
			interceptor.RPCBreakerUnaryInterceptor,
			interceptor.FastlogRPCUnaryInterceptor,
		}

		config.SafeReadServerConfig(func(c *config.ServerConfig) {
			if c.EnabledNats {
				interceptors = append(interceptors, interceptor.NatsRPCFallbackInterceptor)
			}
		})

		checker = healthchecker.NewChecker(&healthchecker.CheckerOptions{
			Discovery:           dis,
			CheckIntervalMs:     c.CheckIntervalMs,
			CheckWorkerPoolSize: c.CheckWorkerPoolSize,
			GRPCDailOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithChainUnaryInterceptor(
					interceptors...,
				),
			},
		})
	})

	sig, err := boot.InitSignal()
	if err != nil {
		log.Fatal(runtimeutil.NewStackErr(err))
	}

	sig.Start()

	log.Println("healthcheckerserver is running...")
	checker.Run()
}
