package handler

import (
	"context"
	"time"

	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/config"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (s *JobSched) ClearExpiredJobs(ctx context.Context, req *jobsched.ClearExpiredJobsReq) (*jobsched.ClearExpiredJobsResp, error) {
	var resp jobsched.ClearExpiredJobsResp

	filter := bson.M{
		"status":             jobsched.SchedJobStatus_SchedJobStatusFinish,
		"last_finish_run_at": time.Now().Unix() - 3600*24*30,
	}
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		if !c.IsProd() {
			delete(filter, "last_finish_run_at")
		}
	})
	model := db.NewJobModel()
	count, err := model.FindCount(context.TODO(), filter)
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}

	if count == 0 {
		return &resp, nil
	}

	if count <= 500 {
		_, err = model.DeleteMany(context.TODO(), filter)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, err
		}
	} else {
		for {
			jobs, err := model.FindManyByPage(ctx, filter, bson.D{{"_id", 1}}, 1, 500, bson.M{"_id": 1})
			if err != nil {
				fastlog.Errorf("err:%v", err)
				return nil, err
			}

			if len(jobs) == 0 {
				break
			}

			var ids []primitive.ObjectID
			for _, job := range jobs {
				ids = append(ids, job.ID)
			}

			_, err = model.DeleteMany(context.TODO(), bson.M{"_id": bson.M{"$in": ids}})
			if err != nil {
				fastlog.Errorf("err:%v", err)
				return nil, err
			}

			time.Sleep(time.Millisecond * 500)
		}
	}

	return &resp, nil
}
