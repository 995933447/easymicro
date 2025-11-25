package interceptor

import (
	"context"
	"runtime/debug"
	"time"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/log"
	"github.com/995933447/fastlog"
	"github.com/995933447/runtimeutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TraceServeRPCUnaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	var trace string
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		traces := md.Get(easymicrogrpc.CtxKeyTrace)
		if len(traces) > 0 {
			trace = traces[0]
			runtimeutil.StoreTrace(trace)
			defer runtimeutil.AutoRemoveTrace()
		}
	}

	if trace == "" {
		trace = runtimeutil.GetOrCreateTrace()
		defer runtimeutil.AutoRemoveTrace()
	}

	if err := grpc.SendHeader(ctx, metadata.Pairs(easymicrogrpc.CtxKeyTrace, trace)); err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func TraceServeRPCStreamInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	var trace string
	md, ok := metadata.FromIncomingContext(ss.Context())
	if ok {
		traces := md.Get(easymicrogrpc.CtxKeyTrace)
		if len(traces) > 0 {
			trace = traces[0]
			runtimeutil.StoreTrace(trace)
			defer runtimeutil.AutoRemoveTrace()
		}
	}

	if trace == "" {
		trace = runtimeutil.GetOrCreateTrace()
		defer runtimeutil.AutoRemoveTrace()
	}

	if err := grpc.SendHeader(ss.Context(), metadata.Pairs(easymicrogrpc.CtxKeyTrace, trace)); err != nil {
		return err
	}

	return handler(srv, ss)
}

func FastlogServeRPCUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	fastlog.Infof("serve-rpc-begin: %s req: %+v", info.FullMethod, req)
	start := time.Now()
	p, ok := peer.FromContext(ctx)
	if !ok {
		p = &peer.Peer{}
	}
	reply, err := handler(ctx, req)
	cost := time.Since(start)
	fastlog.Infof("serve-rpc-end: %s req: %+v resp: %+v err: %v cost: %s, peer addr: %s -> local addr: %s", info.FullMethod, req, reply, err, cost, p.Addr, p.LocalAddr)
	statRes := fastlog.StatResultSuccess
	if err != nil {
		statRes = fastlog.StatResultFail
	}
	fastlog.ReportStat("serve-rpc-"+info.FullMethod, statRes, cost)
	return reply, err
}

type FastlogServeRPCWrappedStream struct {
	fullMethod string
	grpc.ServerStream
}

func (w *FastlogServeRPCWrappedStream) RecvMsg(m any) error {
	err := w.ServerStream.RecvMsg(m)
	fastlog.Infof("serve-rpc-stream: %s receive a message: %+v, err: %v", w.fullMethod, m, err)
	return err
}

func (w *FastlogServeRPCWrappedStream) SendMsg(m any) error {
	err := w.ServerStream.SendMsg(m)
	fastlog.Infof("serve-rpc-stream: %s send a message: %+v, err: %v", w.fullMethod, m, err)
	return err
}

func NewFastlogServeRPCWrappedStream(fullMethod string, stream grpc.ServerStream) grpc.ServerStream {
	return &FastlogServeRPCWrappedStream{fullMethod, stream}
}

func FastlogServeRPCStreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	fastlog.Infof("serve-rpc-stream-begin: %s", info.FullMethod)
	start := time.Now()
	p, ok := peer.FromContext(ss.Context())
	if !ok {
		p = &peer.Peer{}
	}
	err := handler(srv, NewFastlogServeRPCWrappedStream(info.FullMethod, ss))
	cost := time.Since(start)
	fastlog.Infof("serve-rpc-stream-end: %s, cost: %s, err: %v, peer addr: %s -> local addr: %s", info.FullMethod, cost, err, p.Addr, p.LocalAddr)
	statRes := fastlog.StatResultSuccess
	if err != nil {
		statRes = fastlog.StatResultFail
	}
	fastlog.ReportStat("serve-rpc-"+info.FullMethod, statRes, cost)
	return err
}

func RecoveryServeRPCUnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			// 打印错误
			log.Panicf("[PANIC RECOVER] method=%s panic=%v req:%+v", info.FullMethod, r, req)
			// 堆栈
			log.Panicf("[STACK] %s", debug.Stack())
			// 返回 gRPC 错误
			err = status.Errorf(codes.Internal, "server panic: %v", r)
		}
	}()

	reply, err := handler(ctx, req)
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func RecoveryServeRPCStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	var err error
	defer func() {
		if r := recover(); r != nil {
			// 打印错误
			log.Panicf("[PANIC RECOVER] method=%s panic=%v req:%+v", info.FullMethod, r)
			// 堆栈
			log.Panicf("[STACK] %s", debug.Stack())
			// 返回 gRPC 错误
			err = status.Errorf(codes.Internal, "server panic: %v", r)
		}
	}()

	err = handler(srv, ss)
	if err != nil {
		return err
	}

	return nil
}
