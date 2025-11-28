package handler

import (
	"context"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/cache"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"go.mongodb.org/mongo-driver/bson"
)

func (s *JobSched) SetJob(ctx context.Context, req *jobsched.SetJobReq) (*jobsched.SetJobResp, error) {
	var resp jobsched.SetJobResp

	if req.JobId == "" || req.RpcSrv == "" || req.RpcMethod == "" {
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "name and and rpc_server and rpc_method required")
	}

	var nextSchedAt int64
	switch jobsched.SchedJobMode(req.Mode) {
	case jobsched.SchedJobMode_SchedJobModeSpecTime:
		var err error
		nextSchedAt, err = jobsched.CalcNextSchedAt(jobsched.SchedJobMode_SchedJobModeSpecTime, req.SpecTimeAt)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "calc next sched_at error:"+err.Error())
		}
		req.TimeCronExpr = ""
	case jobsched.SchedJobMode_SchedJobModeTimeCron:
		var err error
		nextSchedAt, err = jobsched.CalcNextSchedAt(jobsched.SchedJobMode_SchedJobModeTimeCron, req.TimeCronExpr)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "calc next sched_at error:"+err.Error())
		}
		req.TimeIntervalSec = 0
	case jobsched.SchedJobMode_SchedJobModeTimeIntervalSec:
		var err error
		nextSchedAt, err = jobsched.CalcNextSchedAt(jobsched.SchedJobMode_SchedJobModeTimeIntervalSec, req.TimeIntervalSec)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "calc next sched_at error:"+err.Error())
		}
	default:
		fastlog.Warnf("invalid mode")
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "invalid mode")
	}

	_, err := db.NewJobModel().UpdateOneByID(context.TODO(), req.JobId, bson.M{
		"group_name":                            req.JobGroup,
		"group_max_concur":                      req.JobGroupMaxConcur,
		"mode":                                  req.Mode,
		"next_sched_at":                         nextSchedAt,
		"time_cron_expr":                        req.TimeCronExpr,
		"time_interval_sec":                     req.TimeIntervalSec,
		"allow_re_run_count_caz_fail_per_sched": req.AllowReRunCountCazFailPerSched,
		"rpc_srv":                               req.RpcSrv,
		"rpc_method":                            req.RpcMethod,
		"arg":                                   req.Arg,
		"biz_id":                                req.BizId,
		"max_run_sec":                           req.MaxRunSec,
	})
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	// 没有任务组，不做分组并发限制
	if req.JobGroup == "" {
		return &resp, nil
	}

	jobGrp, exists, err := cache.JobGroupCacheManager.QryWithCache(req.JobGroup)
	if err != nil {
		fastlog.Errorf("er:%v", err)
		return nil, err
	}

	if exists {
		// 并发数没有变化
		if jobGrp.MaxConcur == req.JobGroupMaxConcur {
			return &resp, nil
		}

		err := cache.JobGroupCacheManager.UpdateWithCache(jobGrp.Name, map[string]interface{}{
			"max_concur": req.JobGroupMaxConcur,
		})
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}

		return &resp, nil
	}

	err = db.NewJobGroupModel().InsertOneIgnoreConflict(context.TODO(), &jobsched.JobGroupOrm{
		Name:      req.JobGroup,
		MaxConcur: req.JobGroupMaxConcur,
	})
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	if err := cache.JobGroupCacheManager.IncLeastVersionWithRedis(req.JobGroup); err != nil {
		fastlog.Errorf("err:%v", err)
	}

	return &resp, nil
}
