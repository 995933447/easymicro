package handler

import (
	"context"
	"errors"
	"time"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

func (s *JobSched) HeartBeatAsyncJob(ctx context.Context, req *jobsched.HeartBeatAsyncJobReq) (*jobsched.HeartBeatAsyncJobResp, error) {
	var resp jobsched.HeartBeatAsyncJobResp

	now := time.Now()

	if req.JobId == "" {
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "job_id should not be empty")
	}

	model := db.NewJobModel()

	idObj, toIdObjErr := primitive.ObjectIDFromHex(req.JobId)
	if toIdObjErr != nil {
		fastlog.Errorf("err:%v", toIdObjErr)
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "job_id invalid")
	}

	updated := bson.M{
		"last_heart_beat_in_async_at": now.Unix(),
		"updated_at":                  now,
	}

	if !req.Finish {
		_, err := model.UpdateOne(context.TODO(), bson.M{
			"_id":    idObj,
			"status": jobsched.SchedJobStatus_SchedJobStatusRunningAsync,
		}, updated)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}
		return &resp, nil
	}

	job, err := model.FindOneByID(ctx, req.JobId)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return &resp, nil
		}
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	filter := bson.M{
		"_id": idObj,
		"status": bson.M{
			"$in": []int32{int32(jobsched.SchedJobStatus_SchedJobStatusRunningAsync), int32(jobsched.SchedJobStatus_SchedJobStatusRunning)},
		},
	}
	if req.Err != "" && job.ReRunCountCazFailCurSched < job.AllowReRunCountCazFailPerSched {
		updated["last_fail_at"] = now.Unix()
		updated["last_err"] = req.Err
		updated["status"] = jobsched.SchedJobStatus_SchedJobStatusFail
		inc := bson.M{
			"fail_count": 1,
		}
		fastlog.Infof("update task async run res, cond:%+v, update:%+v, inc:%+v", filter, updated, inc)

		coll, err := model.GetColl()
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}

		res, err := coll.UpdateOne(context.TODO(), filter, bson.M{
			"$set": updated,
			"$inc": inc,
		})
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}

		fastlog.Debugf("res:%+v", res)
		return &resp, nil
	}

	switch jobsched.SchedJobMode(job.Mode) {
	case jobsched.SchedJobMode_SchedJobModeTimeIntervalSec:
		nextSchedAt, err := jobsched.CalcNextSchedAt(jobsched.SchedJobMode_SchedJobModeTimeIntervalSec, job.TimeIntervalSec)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}
		updated["next_sched_at"] = nextSchedAt
		updated["status"] = jobsched.SchedJobStatus_SchedJobStatusNil
	case jobsched.SchedJobMode_SchedJobModeTimeCron:
		nextSchedAt, err := jobsched.CalcNextSchedAt(jobsched.SchedJobMode_SchedJobModeTimeCron, job.TimeCronExpr)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}
		updated["next_sched_at"] = nextSchedAt
		updated["status"] = jobsched.SchedJobStatus_SchedJobStatusNil
	default:
		updated["status"] = jobsched.SchedJobStatus_SchedJobStatusFinish
	}

	if req.Err != "" {
		updated["last_fail_at"] = now.Unix()
		updated["last_err"] = req.Err
		inc := bson.M{
			"fail_count": 1,
		}

		fastlog.Infof("update task async run res, cond:%+v, update:%+v, inc:%+v", filter, updated, inc)

		coll, err := model.GetColl()
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}

		res, err := coll.UpdateOne(context.TODO(), filter, bson.M{
			"$set": updated,
			"$inc": inc,
		})
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}
		fastlog.Debugf("res:%+v", res)
		return &resp, nil
	}

	updated["last_succ_at"] = now.Unix()
	updated["last_res_extra"] = req.Extra
	inc := bson.M{
		"succ_count": 1,
	}

	fastlog.Errorf("update task async run res, cond:%+v, update:%+v, inc:%+v", filter, updated, inc)

	coll, err := model.GetColl()
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	res, err := coll.UpdateOne(context.TODO(), filter, bson.M{
		"$set": updated,
		"$inc": inc,
	})
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	fastlog.Debugf("res:%+v", res)

	return &resp, nil
}
