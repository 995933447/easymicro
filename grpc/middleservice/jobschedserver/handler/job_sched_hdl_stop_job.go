package handler

import (
	"context"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"go.mongodb.org/mongo-driver/bson"
)

func (s *JobSched) StopJob(ctx context.Context, req *jobsched.StopJobReq) (*jobsched.StopJobResp, error) {
	var resp jobsched.StopJobResp

	if req.JobId == "" {
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "job_id is required")
	}

	_, err := db.NewJobModel().UpdateOneByID(context.TODO(), req.JobId, bson.M{
		"status": jobsched.SchedJobStatus_SchedJobStatusStopped,
	})
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	return &resp, nil
}
