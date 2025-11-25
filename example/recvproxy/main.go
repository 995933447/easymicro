package main

import (
	"context"
	"log"

	"github.com/995933447/discovery/manager"
	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/interceptor"
	easymicrogrpcgateway "github.com/995933447/easymicro/grpcgateway"
	"github.com/995933447/easymicro/loader"
	"github.com/995933447/easymicro/node"
	"github.com/995933447/grpcgateway"
	"github.com/jhump/protoreflect/desc"
	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	node.SetName("recvproxy")

	if err := loader.LoadFastlogFromLocal(nil); err != nil {
		log.Fatal(err)
	}

	if err := loader.LoadEtcdFromLocal(); err != nil {
		log.Fatal(err)
	}

	if err := loader.LoadDiscoveryFromLocal(); err != nil {
		log.Fatal(err)
	}

	if err := loader.LoadNatsFromLocal(); err != nil {
		log.Fatal(err)
	}

	err := grpcgateway.Init(&grpcgateway.Conf{
		ServiceName: "recvproxy",
		GrpcConf: grpcgateway.GrpcConf{
			GrpcResolveSchema: easymicrogrpc.DefaultGRPCResolveScheme,
			GrpcClientOptions: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithChainUnaryInterceptor(
					interceptor.TraceRPCUnaryInterceptor,
					interceptor.RPCBreakerUnaryInterceptor,
					interceptor.FastlogRPCUnaryInterceptor,
					interceptor.NatsRPCFallbackInterceptor,
				),
				grpc.WithChainStreamInterceptor(
					interceptor.TraceRPCStreamInterceptor,
					interceptor.RPCBreakerStreamInterceptor,
					interceptor.FastlogRPCStreamInterceptor,
				),
			},
			CallClientTimeoutMs:                5000,
			InitGrpcResolverFunc:               easymicrogrpcgateway.InitGRPCResolverFunc(context.TODO(), easymicrogrpc.DefaultGRPCResolveScheme, manager.DefaultDiscoveryName),
			InitAndWatchGrpcClientMetadataFunc: easymicrogrpcgateway.InitAndWatchGRPCClientMetadataFunc(manager.DefaultDiscoveryName),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("running recvproxy.... listening at:127.0.0.1:8001")

	err = grpcgateway.HandleHttp("127.0.0.1", 8001, grpcgateway.ResolveRpcRouteFromHttp, func(ctx *fasthttp.RequestCtx, method *desc.MethodDescriptor) (interface{}, map[string][]string, []grpc.CallOption, error) {
		return grpcgateway.ResolveRpcParamsFromHttp(ctx, method)
	}, grpcgateway.RespHttp)
	if err != nil {
		panic(err)
	}
}
