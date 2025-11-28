package jobsched

import (
	"context"
	"errors"
	"time"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/gorhill/cronexpr"
	"google.golang.org/grpc"
)

func PrepareGRPC(discoveryName string, dialGRPCOpts ...grpc.DialOption) error {
	if discoveryName == "" {
		discoveryName = EasymicroDiscoveryName
	}

	if err := easymicrogrpc.PrepareDiscoverGRPC(context.TODO(), EasymicroGRPCSchema, discoveryName); err != nil {
		return err
	}

	easymicrogrpc.RegisterServiceDialOpts(EasymicroGRPCPbServiceNameJobSched, true, dialGRPCOpts...)

	return nil
}

func CalcNextSchedAt(mode SchedJobMode, arg interface{}) (int64, error) {
	switch mode {
	case SchedJobMode_SchedJobModeSpecTime:
		argInt64, ok := arg.(int64)
		if !ok {
			return 0, errors.New("arg is not int64")
		}
		return argInt64, nil
	case SchedJobMode_SchedJobModeTimeCron:
		argStr, ok := arg.(string)
		if !ok {
			return 0, errors.New("arg is not string")
		}
		expr, err := cronexpr.Parse(argStr)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return 0, err
		}
		return expr.Next(time.Now()).Unix(), nil
	case SchedJobMode_SchedJobModeTimeIntervalSec:
		argInt, ok := arg.(uint32)
		if !ok {
			return 0, errors.New("arg is not uint32")
		}
		return time.Now().Add(time.Duration(argInt) * time.Second).Unix(), nil
	}

	return 0, errors.New("invalid mode")
}
