package handler

import (
	"context"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"github.com/jinzhu/copier"
	"go.mongodb.org/mongo-driver/bson"
)

func (s *JobSched) GetJobsByBizId(ctx context.Context, req *jobsched.GetJobsByBizIdReq) (*jobsched.GetJobsByBizIdResp, error) {
	var resp jobsched.GetJobsByBizIdResp

	if req.BizId == "" {
		return nil, easymicrogrpc.NewRPCErrWithMsg(jobsched.ErrCode_ErrCodeParamInvalid, "biz_id is required")
	}

	resp.Page = req.Page

	model := db.NewJobModel()
	filter := bson.M{
		"biz_id": req.BizId,
	}
	if req.NeedCalcTotal {
		total, err := model.FindCount(ctx, filter)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}

		resp.Total = uint32(total)
	}

	jobs, err := model.FindManyByPage(ctx, filter, bson.D{{"_id", 1}}, int64(req.Page.Page), int64(req.Page.Size))
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	for _, job := range jobs {
		var data jobsched.Job
		if err = copier.Copy(&data, job); err != nil {
			fastlog.Errorf("err:%v", err)
			continue
		}

		data.JobId = job.ID.Hex()
		resp.Jobs = append(resp.Jobs, &data)
	}

	return &resp, nil
}
