package handler

import (
	"context"
	"errors"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"github.com/jinzhu/copier"
	"go.mongodb.org/mongo-driver/mongo"
)

func (s *JobSched) GetJob(ctx context.Context, req *jobsched.GetJobReq) (*jobsched.GetJobResp, error) {
	var resp jobsched.GetJobResp

	if req.JobId == "" {
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "job_id is required")
	}

	job, err := db.NewJobModel().FindOneByID(ctx, req.JobId)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, easymicrogrpc.NewRPCErr(jobsched.ErrCode_ErrCodeJobNotFound)
		}
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	resp.Job = &jobsched.Job{}
	if err = copier.Copy(resp.Job, job); err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	resp.Job.JobId = job.ID.Hex()

	return &resp, nil
}
