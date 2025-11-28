package grpc

import (
	"context"

	"github.com/995933447/fastlog"
	"google.golang.org/grpc/stats"
)

var _ stats.Handler = (*FastlogRPCStatsHandler)(nil)

type FastlogRPCStatsHandler struct {
}

func (f FastlogRPCStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

func (f FastlogRPCStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
}

func (f FastlogRPCStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	fastlog.Infof("local address:%s request remote address:%s", info.LocalAddr.String(), info.RemoteAddr.String())
	return ctx
}

func (f FastlogRPCStatsHandler) HandleConn(ctx context.Context, connStats stats.ConnStats) {
}
