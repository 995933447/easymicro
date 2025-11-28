package looptask

import (
	"context"
	"errors"
	"time"

	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/jobexec"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"github.com/995933447/looptask"
	"github.com/995933447/runtimeutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"google.golang.org/grpc"
)

var _ looptask.Task = (*Job)(nil)
var _ looptask.SubTask = (*Job)(nil)

type Job struct {
	Id                primitive.ObjectID
	dataObj           *jobsched.JobOrm
	err               error
	execJobResp       *jobexec.ExecJobResp
	attempted         uint32
	lastCallbackTrace string
}

func (j *Job) incAttempted() error {
	defer func() {
		j.attempted++
	}()

	now := time.Now()
	updated := bson.M{
		"$set": bson.M{
			"updated_at":  now,
			"status":      jobsched.SchedJobStatus_SchedJobStatusRunning,
			"last_run_at": now.Unix(),
		},
		"$inc": bson.M{
			"run_count": 1,
		},
	}

	if j.attempted > 0 {
		inc := updated["$inc"].(bson.M)
		inc["re_run_count_caz_fail_cur_sched"] = 1
		inc["re_run_count_caz_fail"] = 1
	}

	filter := bson.M{
		"_id": j.dataObj.ID,
	}

	fastlog.Infof("update task to re running, cond:%+v, update:%+v", filter, updated)
	coll, err := db.NewJobModel().GetColl()
	if err != nil {
		fastlog.Errorf("get job model err:%v", err)
		return err
	}

	_, err = coll.UpdateOne(context.TODO(), filter, updated)
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (j *Job) Exec() (nextInterval time.Duration, retry bool) {
	j.lastCallbackTrace = runtimeutil.CreateTrace()
	defer runtimeutil.AutoRemoveTrace()

	fastlog.Infof("exec job, %+v", j.dataObj)

	err := j.incAttempted()
	if err != nil {
		j.err = err
		fastlog.Errorf("err:%v", err)
		return 0, false
	}

	opts := easymicrogrpc.BuildServiceDialOpts(j.dataObj.RpcSrv)
	conn, err := grpc.NewClient(jobsched.EasymicroGRPCSchema+":///"+j.dataObj.RpcSrv, opts...)
	if err != nil {
		j.err = err
		fastlog.Errorf("err:%v", err)
		return 0, false
	}

	cli := jobexec.NewJobExecClient(conn)
	execTimeoutSec := 5
	if j.dataObj.MaxRunSec > 0 {
		execTimeoutSec = int(j.dataObj.MaxRunSec)
	}
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*time.Duration(execTimeoutSec))

	defer cancel()

	j.execJobResp, err = cli.ExecJob(ctx, &jobexec.ExecJobReq{
		Arg:         j.dataObj.Arg,
		JobId:       j.dataObj.ID.Hex(),
		JobName:     j.dataObj.Name,
		BizId:       j.dataObj.BizId,
		JobGroup:    j.dataObj.GroupName,
		Attempted:   j.attempted,
		Mode:        j.dataObj.Mode,
		MaxRunSec:   j.dataObj.MaxRunSec,
		PlanSchedAt: j.dataObj.NextSchedAt,
		RpcSrv:      j.dataObj.RpcSrv,
		RpcMethod:   j.dataObj.RpcMethod,
	})
	if err != nil {
		fastlog.Importantf("run job err:%v", err)
		j.err = err
		return 0, false
	}

	return 0, false
}

func (j *Job) Ready() bool {
	fastlog.Debugf("job going ready, job:%+v", j.dataObj)
	var err error
	model := db.NewJobModel()
	j.dataObj, err = model.FindOneByID(context.TODO(), j.Id.Hex())
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return false
	}

	if j.dataObj.Status != int32(jobsched.SchedJobStatus_SchedJobStatusSched) && j.dataObj.Status != int32(jobsched.SchedJobStatus_SchedJobStatusFail) {
		fastlog.Importantf("job is not sched status or async run failed, current status:%d", j.dataObj.Status)
		return false
	}

	if j.dataObj.Status == int32(jobsched.SchedJobStatus_SchedJobStatusFail) {
		j.attempted = j.dataObj.ReRunCountCazFailCurSched + 1
	}

	now := time.Now()
	updated := bson.M{
		"$set": bson.M{
			"updated_at":  now,
			"status":      jobsched.SchedJobStatus_SchedJobStatusRunning,
			"last_run_at": now.Unix(),
		},
	}
	filter := bson.M{
		"_id":    j.dataObj.ID,
		"status": j.dataObj.Status,
	}
	fastlog.Infof("change task to running, cond:%+v, update:%+v", filter, updated)
	coll, err := db.NewJobModel().GetColl()
	if err != nil {
		fastlog.Errorf("get job model err:%v", err)
		return false
	}
	res, err := coll.UpdateOne(context.TODO(), filter, updated)
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return false
	}

	if res.MatchedCount == 0 {
		fastlog.Importantf("task is not ready status", j.dataObj.ID.Hex())
		return false
	}

	return true
}

func (j *Job) GetSubTaskConcur() uint32 {
	return 1
}

func (j *Job) NextBatchSubTasks() []looptask.SubTask {
	return []looptask.SubTask{j}
}

func (j *Job) OnProceed() {
	model := db.NewJobModel()
	now := time.Now()
	calcNextSchedState := func() (jobsched.SchedJobStatus, int64, error) {
		switch jobsched.SchedJobMode(j.dataObj.Mode) {
		case jobsched.SchedJobMode_SchedJobModeSpecTime:
			return jobsched.SchedJobStatus_SchedJobStatusFinish, 0, nil
		case jobsched.SchedJobMode_SchedJobModeTimeCron:
			nextSchedAt, err := jobsched.CalcNextSchedAt(jobsched.SchedJobMode_SchedJobModeTimeCron, j.dataObj.TimeCronExpr)
			if err != nil {
				fastlog.Errorf("err:%v", err)
				return 0, 0, err
			}

			return jobsched.SchedJobStatus_SchedJobStatusNil, nextSchedAt, nil
		case jobsched.SchedJobMode_SchedJobModeTimeIntervalSec:
			nextSchedAt, err := jobsched.CalcNextSchedAt(jobsched.SchedJobMode_SchedJobModeTimeIntervalSec, j.dataObj.TimeIntervalSec)
			if err != nil {
				fastlog.Errorf("err:%v", err)
				return 0, 0, err
			}
			return jobsched.SchedJobStatus_SchedJobStatusNil, nextSchedAt, nil
		}

		return 0, 0, errors.New("invalid schedule mode")
	}

	if j.err != nil {
		filter := bson.M{
			"_id": j.dataObj.ID,
		}

		updated := bson.M{
			"$set": bson.M{
				"last_err":            j.err.Error(),
				"last_fail_at":        now.Unix(),
				"last_finish_run_at":  now.Unix(),
				"updated_at":          now,
				"last_callback_trace": j.lastCallbackTrace,
			},
			"$inc": bson.M{
				"fail_count": 1,
			},
		}

		set := updated["$set"].(bson.M)
		if j.attempted > j.dataObj.AllowReRunCountCazFailPerSched {
			status, nextSchedAt, err := calcNextSchedState()
			if err != nil {
				fastlog.Errorf("err:%v", err)
				return
			}
			set["status"] = status
			if status != jobsched.SchedJobStatus_SchedJobStatusFinish {
				set["next_sched_at"] = nextSchedAt
			}
		} else {
			set["status"] = jobsched.SchedJobStatus_SchedJobStatusFail
		}

		fastlog.Infof("record task fail, cond:%+v, update:%+v", filter, updated)
		coll, err := db.NewJobModel().GetColl()
		if err != nil {
			fastlog.Errorf("get job model err:%v", err)
			return
		}

		_, err = coll.UpdateOne(context.TODO(), filter, updated)
		if err != nil {
			fastlog.Errorf("err:%v", err)
		}
		return
	}

	if j.execJobResp.IsRunInAsync {
		filter := bson.M{
			"_id":    j.dataObj.ID,
			"status": jobsched.SchedJobStatus_SchedJobStatusRunning,
		}
		_, err := model.UpdateOne(context.TODO(), filter, bson.M{
			"is_run_in_async":             1,
			"updated_at":                  now,
			"last_heart_beat_in_async_at": now.Unix(),
			"last_finish_run_at":          now.Unix(),
			"last_res_extra":              j.execJobResp.Extra,
			"status":                      jobsched.SchedJobStatus_SchedJobStatusRunningAsync,
			"last_callback_trace":         j.lastCallbackTrace,
		})
		if err != nil {
			fastlog.Errorf("err:%v", err)
		}
		return
	}

	updated := bson.M{
		"updated_at": now.Unix(),
	}

	filter := bson.M{
		"_id": j.Id,
	}

	status, nextSchedAt, err := calcNextSchedState()
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return
	}
	updated["status"] = status
	if status != jobsched.SchedJobStatus_SchedJobStatusFinish {
		updated["next_sched_at"] = nextSchedAt
	}
	updated["last_finish_run_at"] = now.Unix()
	updated["last_succ_at"] = now.Unix()
	updated["is_run_in_async"] = 0
	updated["last_res_extra"] = j.execJobResp.Extra
	updated["last_callback_trace"] = j.lastCallbackTrace
	fastlog.Infof("change task to proceed, cond:%+v, update:%+v", filter, updated)
	coll, err := db.NewJobModel().GetColl()
	if err != nil {
		fastlog.Errorf("get job model err:%v", err)
		return
	}
	_, err = coll.UpdateOne(context.TODO(), filter, bson.M{
		"$set": updated,
		"$inc": bson.M{
			"succ_count": 1,
		},
	})
	if err != nil {
		fastlog.Errorf("err:%v", err)
	}

	return
}
