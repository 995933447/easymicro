package handler

import (
	"context"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"go.mongodb.org/mongo-driver/bson"
)

func (s *JobSched) StopJobByUniqKey(ctx context.Context, req *jobsched.StopJobByUniqKeyReq) (*jobsched.StopJobByUniqKeyResp, error) {
	var resp jobsched.StopJobByUniqKeyResp

	if req.UniqKey == "" {
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "uniq_key required")
	}

	_, err := db.NewJobModel().UpdateOne(context.TODO(), bson.M{
		"uniq_key": req.UniqKey,
	}, bson.M{
		"status": jobsched.SchedJobStatus_SchedJobStatusStopped,
	})
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	return &resp, nil
}
