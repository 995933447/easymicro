package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobexec"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/fastlog"
	"github.com/995933447/runtimeutil"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type ExecJobCallback struct {
	execMethod     ExecJobFunc
	isRunInAsync   bool
	execMethodDesc protoreflect.MethodDescriptor
}

var execJobCallbacks sync.Map

func RegisterJobCallback(serviceName, method string, callback ExecJobFunc, isRunInAsync bool) error {
	methodDesc, ok := easymicrogrpc.FindMethodDescriptor(serviceName, method)
	if !ok {
		return fmt.Errorf("not found service:%s.method:%s descriptor from protoregistry", serviceName, method)
	}

	execJobCallbacks.Store(genExecJobCallbackKey(serviceName, method), &ExecJobCallback{
		execMethod:     callback,
		isRunInAsync:   isRunInAsync,
		execMethodDesc: methodDesc,
	})
	return nil
}

func GetExecJobCallback(serviceName, method string) (*ExecJobCallback, bool) {
	callback, ok := execJobCallbacks.Load(genExecJobCallbackKey(serviceName, method))
	if !ok {
		return nil, false
	}
	return callback.(*ExecJobCallback), true
}

func genExecJobCallbackKey(srvName string, method string) string {
	return fmt.Sprintf("%s.%s", srvName, method)
}

type ExecJobFunc func(ctx context.Context, message proto.Message) (proto.Message, error)

type JobExecutor struct {
	jobexec.UnimplementedJobExecServer
}

func (s *JobExecutor) ExecJob(ctx context.Context, req *jobexec.ExecJobReq) (*jobexec.ExecJobResp, error) {
	var resp jobexec.ExecJobResp

	cb, ok := GetExecJobCallback(req.RpcSrv, req.RpcMethod)
	if !ok {
		return nil, easymicrogrpc.NewRPCErr(jobexec.ErrCode_ErrCodeCallbackFuncNotFound)
	}

	cbReqDyn, _ := easymicrogrpc.NewDynamicMessagesFromMethod(cb.execMethodDesc)

	if err := protojson.Unmarshal([]byte(req.Arg), cbReqDyn); err != nil {
		return nil, err
	}

	cbReq, err := easymicrogrpc.ConvertDynamicToReal(cb.execMethodDesc, cbReqDyn)
	if err != nil {
		return nil, err
	}

	if !cb.isRunInAsync {
		cbResp, err := cb.execMethod(ctx, cbReq)
		if err != nil {
			return nil, err
		}

		var cbRespStr string
		b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(cbResp)
		if err != nil {
			fastlog.Error(err)
			cbRespStr = fmt.Sprintf("%+v", cbResp)
		} else {
			cbRespStr = string(b)
		}

		resp.Extra = cbRespStr
		return &resp, nil
	}

	runtimeutil.Go(func() {
		stoppedCh := make(chan struct{})
		runtimeutil.Go(func() {
			heartBeatTk := time.NewTicker(time.Second * 3)
			defer heartBeatTk.Stop()
			for {
				select {
				case <-heartBeatTk.C:
					_, err := jobsched.JobSchedGRPC().HeartBeatAsyncJob(ctx, &jobsched.HeartBeatAsyncJobReq{
						JobId: req.JobId,
					})
					if err != nil {
						fastlog.Errorf("err:%v", err)
					}
				case <-stoppedCh:
					goto out
				}
			}
		out:
			return
		})

		heartBeatReq := &jobsched.HeartBeatAsyncJobReq{
			JobId: req.JobId,
		}

		md, _ := metadata.FromIncomingContext(ctx)
		cbCtx := metadata.NewIncomingContext(context.TODO(), md)
		cbResp, err := cb.execMethod(cbCtx, cbReq)
		fastlog.Infof("run job(%+v) failed, err:%+v", cbReq, err)
		stoppedCh <- struct{}{}
		heartBeatReq.Finish = true
		if err != nil {
			heartBeatReq.Err = err.Error()
		} else {
			var cbRespStr string
			b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(cbResp)
			if err != nil {
				fastlog.Error(err.Error())
				cbRespStr = fmt.Sprintf("%+v", cbResp)
			} else {
				cbRespStr = string(b)
			}
			heartBeatReq.Extra = cbRespStr
		}
		_, err = jobsched.JobSchedGRPC().HeartBeatAsyncJob(context.TODO(), heartBeatReq)
		if err != nil {
			fastlog.Errorf("err:%v", err)
		}
	})

	resp.IsRunInAsync = true

	return &resp, nil
}
