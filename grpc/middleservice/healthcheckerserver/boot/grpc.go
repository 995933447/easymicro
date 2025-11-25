package boot

import (
	"fmt"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/interceptor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func RegisterGRPCDialOpts() {
	unaryInterceptors := []grpc.UnaryClientInterceptor{
		interceptor.TraceRPCUnaryInterceptor,
		interceptor.RPCBreakerUnaryInterceptor,
	}

	easymicrogrpc.RegisterGlobalDialOpts(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, easymicrogrpc.BalancerNameRoundRobin)),
		grpc.WithChainUnaryInterceptor(unaryInterceptors...),
		grpc.WithChainStreamInterceptor(
			interceptor.TraceRPCStreamInterceptor,
			interceptor.RPCBreakerStreamInterceptor,
		),
	)
}
