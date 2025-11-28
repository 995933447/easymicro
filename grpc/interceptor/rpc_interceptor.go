package interceptor

import (
	"context"
	"runtime/debug"
	"time"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/log"
	"github.com/995933447/easymicro/nats"
	"github.com/995933447/fastlog"
	"github.com/995933447/rpcbreaker"
	"github.com/995933447/runtimeutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func NatsRPCFallbackInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	err := invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.Unavailable, codes.Unimplemented:
				svc, m := easymicrogrpc.ParseGRPCMethod(method)
				log.Infof("[NATS RPC fallback] method=%s, svc=%s, err=%v", method, svc, err)
				timeout := time.Second * 5
				if deadline, ok := ctx.Deadline(); ok {
					if t := time.Until(deadline); t > 0 {
						timeout = t
					}
				}
				ne := nats.CallLikeGRPC(ctx, svc, m, req.(proto.Message), reply.(proto.Message), timeout)
				if ne != nil {
					_, ok = status.FromError(ne)
					if ok {
						err = ne
					} else {
						log.Error(ne)
					}
				} else {
					return nil
				}
			}
		}
	}
	return err
}

func RPCBreakerUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	serviceName, methodName := easymicrogrpc.ParseGRPCMethod(method)

	var err error
	rpcbreaker.DoWithBreaker(serviceName, methodName, func() []interface{} {
		err = invoker(ctx, method, req, reply, cc, opts...)
		return []interface{}{reply, err}
	}, req)

	return err
}

func RPCBreakerStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	serviceName, methodName := easymicrogrpc.ParseGRPCMethod(method)

	var (
		err    error
		stream grpc.ClientStream
	)
	rpcbreaker.DoWithBreaker(serviceName, methodName, func() []interface{} {
		stream, err = streamer(ctx, desc, cc, method, opts...)
		return []interface{}{stream, err}
	})

	return stream, err
}

func TraceRPCUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	trace := runtimeutil.GetOrCreateTrace()
	defer runtimeutil.AutoRemoveTrace()
	return invoker(metadata.AppendToOutgoingContext(ctx, easymicrogrpc.CtxKeyTrace, trace), method, req, reply, cc, opts...)
}

func TraceRPCStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	trace := runtimeutil.GetOrCreateTrace()
	defer runtimeutil.AutoRemoveTrace()
	return streamer(metadata.AppendToOutgoingContext(ctx, easymicrogrpc.CtxKeyTrace, trace), desc, cc, method, opts...)
}

func FastlogRPCUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	m, _ := metadata.FromOutgoingContext(ctx)
	start := time.Now()
	var p peer.Peer
	opts = append(opts, grpc.Peer(&p))
	err := invoker(ctx, method, req, reply, cc, opts...)
	cost := time.Since(start)
	fastlog.Infof("rpc: %s, start time: %s, cost: %s, err: %v req: %+v resp: %+v, peer:local addr:%s -> remote addr:%s, metadata: %+v", method, start.Format(time.RFC3339), cost, err, req, reply, p.LocalAddr, p.Addr, m)
	statRes := fastlog.StatResultSuccess
	if err != nil {
		statRes = fastlog.StatResultFail
	}
	fastlog.ReportStat("rpc-"+method, statRes, cost)
	return err
}

// FastlogRPCWrappedStream  wraps around the embedded grpc.ClientStream, and intercepts the RecvMsg and
// SendMsg method call.
type FastlogRPCWrappedStream struct {
	method string
	grpc.ClientStream
}

func (w *FastlogRPCWrappedStream) RecvMsg(m any) error {
	err := w.ClientStream.RecvMsg(m)
	fastlog.Infof("rpc-stream: %s receive a message: %+v, err: %v", w.method, m, err)
	return err
}

func (w *FastlogRPCWrappedStream) SendMsg(m any) error {
	err := w.ClientStream.SendMsg(m)
	fastlog.Infof("rpc-stream: %s send a message: %+v, err: %v", w.method, m, err)
	return err
}

func NewFastlogRPCWrappedStream(method string, s grpc.ClientStream) grpc.ClientStream {
	return &FastlogRPCWrappedStream{method: method, ClientStream: s}
}

func FastlogRPCStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	m, _ := metadata.FromOutgoingContext(ctx)
	start := time.Now()
	var p peer.Peer
	opts = append(opts, grpc.Peer(&p))
	s, err := streamer(ctx, desc, cc, method, opts...)
	cost := time.Since(start)
	fastlog.Infof("rpc-stream: %s, start time: %s, cost: %s, err: %v, peer:local addr:%s -> remote addr:%s, metadata:%+v", method, start.Format(time.RFC3339), cost, err, p.LocalAddr, p.Addr, m)
	statRes := fastlog.StatResultSuccess
	defer fastlog.ReportStat("rpc-"+method, statRes, cost)
	if err != nil {
		statRes = fastlog.StatResultFail
		return nil, err
	}
	return NewFastlogRPCWrappedStream(method, s), nil
}

func RecoveryRPCUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	var err error
	defer func() {
		if r := recover(); r != nil {
			// 打印错误
			log.Panicf("[PANIC RECOVER] method=%s panic=%v req:%+v", method, r, req)
			// 堆栈
			log.Panicf("[STACK] %s", debug.Stack())
			// 返回 gRPC 错误
			err = status.Errorf(codes.Internal, "server panic: %v", r)
		}
	}()

	err = invoker(ctx, method, req, reply, cc, opts...)
	if err != nil {
		return err
	}

	return nil
}

func RecoveryRPCStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			// 打印错误
			log.Panicf("[PANIC RECOVER] method=%s panic=%v", method, r)
			// 堆栈
			log.Panicf("[STACK] %s", debug.Stack())
			// 返回 gRPC 错误
			err = status.Errorf(codes.Internal, "server panic: %v", r)
		}
	}()

	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}

	return s, nil
}
