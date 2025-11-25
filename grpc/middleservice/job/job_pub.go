package job

import (
	"context"

	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type BatchPubReqConfig struct {
	One *jobsched.PubJobReq
	Arg proto.Message
}

type BatchPubReq struct {
	Configs []*BatchPubReqConfig
}

func (r *BatchPubReq) Add(one *jobsched.PubJobReq, arg proto.Message) *BatchPubReq {
	r.Configs = append(r.Configs, &BatchPubReqConfig{
		One: one,
		Arg: arg,
	})
	return r
}

func NewBatchPubReq() *BatchPubReq {
	return &BatchPubReq{}
}

func BatchPub[RQ, RP proto.Message](ctx context.Context, req *BatchPubReq, callback func(context.Context, RQ) (RP, error), isRunInAsync bool) error {
	for _, cfg := range req.Configs {
		if _, err := Pub(ctx, cfg.One, cfg.Arg, callback, isRunInAsync); err != nil {
			return err
		}
	}
	return nil
}

func BatchPubSameRPC[RQ, RP proto.Message](ctx context.Context, rpcSrv, rpcMethod string, req *BatchPubReq, callback func(context.Context, RQ) (RP, error), isRunInAsync bool) error {
	for _, cfg := range req.Configs {
		cfg.One.RpcSrv = rpcSrv
		cfg.One.RpcMethod = rpcMethod
		if _, err := Pub(ctx, cfg.One, cfg.Arg, callback, isRunInAsync); err != nil {
			return err
		}
	}
	return nil
}

func Pub[RQ, RP proto.Message](ctx context.Context, req *jobsched.PubJobReq, arg proto.Message, callback func(context.Context, RQ) (RP, error), isRunInAsync bool) (*jobsched.PubJobResp, error) {
	b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(arg)
	if err != nil {
		return nil, err
	}

	req.Arg = string(b)

	if err = RegisterJobCallback(req.RpcSrv, req.RpcMethod, func(ctx context.Context, message proto.Message) (proto.Message, error) {
		return callback(ctx, message.(RQ))
	}, isRunInAsync); err != nil {
		return nil, err
	}

	return jobsched.JobSchedGRPC().PubJob(ctx, req)
}
