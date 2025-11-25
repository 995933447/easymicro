package handler

import (
	"context"
	"errors"

	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/cache"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"github.com/995933447/runtimeutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func (s *JobSched) PubJob(ctx context.Context, req *jobsched.PubJobReq) (*jobsched.PubJobResp, error) {
	var resp jobsched.PubJobResp

	if req.Name == "" || req.RpcSrv == "" || req.RpcMethod == "" {
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "name or and rpc_server or rpc_method required")
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
		req.TimeIntervalSec = 0
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
		req.TimeCronExpr = ""
	default:
		fastlog.Warn("invalid mode")
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "invalid mode")
	}

	jobModel := db.NewJobModel()
	if req.UniqKey != "" {
		upsert := bson.M{
			"name":                                  req.Name,
			"status":                                jobsched.SchedJobStatus_SchedJobStatusNil,
			"mode":                                  req.Mode,
			"next_sched_at":                         nextSchedAt,
			"allow_re_run_count_caz_fail_per_sched": req.AllowReRunCountCazFailPerSched,
			"max_run_sec":                           req.MaxRunSec,
			"group_name":                            req.JobGroup,
			"group_max_concur":                      req.JobGroupMaxConcur,
			"time_cron_expr":                        req.TimeCronExpr,
			"time_interval_sec":                     req.TimeIntervalSec,
			"rpc_srv":                               req.RpcSrv,
			"rpc_method":                            req.RpcMethod,
			"arg":                                   req.Arg,
			"biz_id":                                req.BizId,
			"uniq_key":                              req.UniqKey,
			"creator_trace":                         runtimeutil.GetTrace(),
		}
		oldJob, err := jobModel.FindOne(ctx, bson.M{
			"uniq_key": req.UniqKey,
		})
		if err != nil {
			if !errors.Is(err, mongo.ErrNoDocuments) {
				fastlog.Errorf("err:%v", err)
				return nil, err
			}
		} else {
			resp.JobId = oldJob.ID.Hex()
			// 原任务不是停止或者完成状态状态，继承原任务状态
			if oldJob.Status != int32(jobsched.SchedJobStatus_SchedJobStatusStopped) && oldJob.Status != int32(jobsched.SchedJobStatus_SchedJobStatusFinish) {
				upsert["status"] = oldJob.Status
				if oldJob.Mode == req.Mode && oldJob.TimeIntervalSec == req.TimeIntervalSec && oldJob.TimeCronExpr == req.TimeCronExpr {
					upsert["next_sched_at"] = oldJob.NextSchedAt
				}
			}
		}

		_, err = jobModel.Upsert(context.TODO(), bson.M{
			"uniq_key": req.UniqKey,
		}, upsert)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}

		if resp.JobId == "" {
			job, err := jobModel.FindOne(ctx, bson.M{
				"uniq_key": req.UniqKey,
			})
			if err != nil {
				fastlog.Errorf("err:%v", err)
				return nil, err
			}

			resp.JobId = job.ID.Hex()
		}
	} else {
		dataObj := jobsched.JobOrm{
			Name:                           req.Name,
			Status:                         int32(jobsched.SchedJobStatus_SchedJobStatusNil),
			Mode:                           req.Mode,
			NextSchedAt:                    nextSchedAt,
			AllowReRunCountCazFailPerSched: req.AllowReRunCountCazFailPerSched,
			MaxRunSec:                      req.MaxRunSec,
			GroupName:                      req.JobGroup,
			GroupMaxConcur:                 req.JobGroupMaxConcur,
			TimeCronExpr:                   req.TimeCronExpr,
			TimeIntervalSec:                req.TimeIntervalSec,
			RpcSrv:                         req.RpcSrv,
			RpcMethod:                      req.RpcMethod,
			Arg:                            req.Arg,
			BizId:                          req.BizId,
			UniqKey:                        req.UniqKey,
			CreatorTrace:                   runtimeutil.GetTrace(),
		}
		err := jobModel.InsertOne(context.TODO(), &dataObj)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}

		resp.JobId = dataObj.ID.Hex()
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
