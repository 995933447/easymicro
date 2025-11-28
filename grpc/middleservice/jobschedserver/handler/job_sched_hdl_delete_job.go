package handler

import (
	"context"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
)

func (s *JobSched) DeleteJob(ctx context.Context, req *jobsched.DeleteJobReq) (*jobsched.DeleteJobResp, error) {
	var resp jobsched.DeleteJobResp

	if req.JobId == "" {
		return nil, grpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "job_id is empty")
	}

	_, err := db.NewJobModel().DeleteOneByID(ctx, req.JobId)
	if err != nil {
		fastlog.Error(err)
		return nil, err
	}

	return &resp, nil
}
