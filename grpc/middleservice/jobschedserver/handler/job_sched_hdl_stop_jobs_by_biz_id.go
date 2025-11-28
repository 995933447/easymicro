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

func (s *JobSched) StopJobsByBizId(ctx context.Context, req *jobsched.StopJobsByBizIdReq) (*jobsched.StopJobsByBizIdResp, error) {
	var resp jobsched.StopJobsByBizIdResp

	if req.BizId == "" {
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "biz_id is required")
	}

	filter := bson.M{
		"biz_id": req.BizId,
		"status": bson.M{
			"$ne": jobsched.SchedJobStatus_SchedJobStatusFinish,
		},
	}

	model := db.NewJobModel()
	count, err := model.FindCount(ctx, filter)
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	if count == 0 {
		return &resp, nil
	}

	updated := bson.M{
		"status": jobsched.SchedJobStatus_SchedJobStatusStopped,
	}

	if count <= 500 {
		_, err = model.UpdateMany(context.TODO(), filter, updated)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		resp.HasMore = true
		jobs, err := model.FindManyByPage(ctx, filter, bson.D{{"_id", 1}}, 1, 500, bson.M{"_id": 1})
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
