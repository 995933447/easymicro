package handler

import (
	"context"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (s *JobSched) SetJobsByBizId(ctx context.Context, req *jobsched.SetJobsByBizIdReq) (*jobsched.SetJobsByBizIdResp, error) {
	var resp jobsched.SetJobsByBizIdResp

	if req.BizId == "" || req.RpcSrv == "" || req.RpcMethod == "" {
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "biz_id and rpc_srv and rpc_method required")
	}

	model := db.NewJobModel()
	count, err := model.FindCount(context.TODO(), bson.M{
		"biz_id": req.BizId,
	})
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	if count == 0 {
		return &resp, nil
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
		fastlog.Warn("invalid mode")
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "invalid mode")
	}

	updated := bson.M{
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
		"max_run_sec":                           req.MaxRunSec,
	}

	if count <= 500 {
		_, err = model.UpdateMany(context.TODO(), bson.M{
			"biz_id": req.BizId,
		}, updated)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		resp.HasMore = true
		jobs, err := model.FindManyByPage(context.TODO(), bson.M{"biz_id": req.BizId}, bson.D{{"_id", 1}}, 1, 500, bson.M{"_id": 1})
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}

		var ids []primitive.ObjectID
		for _, job := range jobs {
			ids = append(ids, job.ID)
		}

		_, err = model.UpdateMany(context.TODO(), bson.M{"_id": bson.M{"$in": ids}}, updated)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}
	}

	return &resp, nil
}
