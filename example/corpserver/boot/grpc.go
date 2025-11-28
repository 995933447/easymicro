package boot

import (
	"fmt"

	"github.com/995933447/easymicro/example/corpserver/config"
	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/interceptor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func RegisterGRPCDialOpts() {
	unaryInterceptors := []grpc.UnaryClientInterceptor{
		interceptor.RecoveryRPCUnaryInterceptor,
		interceptor.TraceRPCUnaryInterceptor,
		interceptor.RPCBreakerUnaryInterceptor,
		interceptor.FastlogRPCUnaryInterceptor,
	}
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		if !c.IsProd() {
			unaryInterceptors = append(unaryInterceptors, interceptor.NatsRPCFallbackInterceptor)
		}
	})

	easymicrogrpc.RegisterGlobalDialOpts(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, easymicrogrpc.BalancerNameRoundRobin)),
		grpc.WithChainUnaryInterceptor(unaryInterceptors...),
		grpc.WithChainStreamInterceptor(
			interceptor.TraceRPCStreamInterceptor,
			interceptor.RPCBreakerStreamInterceptor,
			interceptor.FastlogRPCStreamInterceptor,
			interceptor.RecoveryRPCStreamInterceptor,
		),
	)
}
