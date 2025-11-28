package looptask

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/995933447/easymicro/elect"
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/cache"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/config"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"github.com/995933447/looptask"
	uuid "github.com/satori/go.uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// DefJobGrp 生成一个默认的作业组，没有作业组的作业都分配到该组下，并且不做并发限制
var DefJobGrp = uuid.NewV4().String()

func InitJobSched() error {
	sched, err := looptask.NewSched(looptask.SchedCfg{
		Name:                     fmt.Sprintf("easymicrojob.%s", jobsched.EasymicroGRPCPbServiceNameJobSched),
		ConcurTaskMax:            40,
		NewTaskHdl:               NewJob,
		RefreshTasksHdl:          RefreshJobs,
		CheckTaskStoppedHdl:      CheckJobStopped,
		RefreshTaskIntervalMs:    1000,
		CheckTasksStopIntervalMs: 1000,
		SubTaskMaxAttempt:        1,
		Elect:                    elect.MustGetElect(),
	})
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return err
	}

	initJobGroupConcur(sched)

	sched.Run()

	return nil
}

var setJobGrpConcurMu sync.Mutex

// 设置作业组最大并发限制
func initJobGroupConcur(sched *looptask.Sched) {
	// 监听作业组数据变化，设置作业组并发限制
	cache.JobGroupCacheManager.OnJobGroupChanged(func(grpName string) {
		jobGrp, exists, err := cache.JobGroupCacheManager.QryWithCache(grpName)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return
		}
		if !exists {
			return
		}
		setJobGrpConcurMu.Lock()
		sched.SetGrpConcur(jobGrp.Name, int32(jobGrp.MaxConcur))
		setJobGrpConcurMu.Unlock()
	})

	// 初始化作业组并发控制
	var (
		page     = int64(1)
		pageSize = int64(500)
	)
	for {
		jobGroups, err := db.NewJobGroupModel().FindManyByPage(context.TODO(), bson.M{}, bson.D{{"_id", 1}}, page, pageSize, bson.M{"name": 1, "_id": 0, "max_concur": 1})
		if err != nil {
			fastlog.Errorf("err:%v", err)
			continue
		}

		if len(jobGroups) == 0 {
			break
		}

		page++

		for _, jobGroup := range jobGroups {
			_, limited := sched.GetGrpConcur(jobGroup.Name)
			if limited {
				continue
			}

			leastJobGrp, _, exists, err := cache.JobGroupCacheManager.QryFromRedis(jobGroup.Name)
			if err != nil {
				fastlog.Errorf("err:%v", err)
			}

			if err == nil && exists {
				jobGroup = leastJobGrp
			}

			setJobGrpConcurMu.Lock()
			_, limited = sched.GetGrpConcur(jobGroup.Name)
			if limited {
				continue
			}
			sched.SetGrpConcur(jobGroup.Name, int32(jobGroup.MaxConcur))
			setJobGrpConcurMu.Unlock()
		}
	}
}

func CheckJobStopped(jobIds []string) (stoppedJobIds []string) {
	model := db.NewJobModel()

	for len(jobIds) > 0 {
		var batch []string
		if len(jobIds) > 500 {
			batch = jobIds[:500]
			jobIds = nil
		} else {
			batch = jobIds[:]
			jobIds = nil
		}
		var idObjs []primitive.ObjectID
		for _, jobId := range batch {
			idObj, err := primitive.ObjectIDFromHex(jobId)
			if err != nil {
				fastlog.Errorf("err:%v", err)
				continue
			}
			idObjs = append(idObjs, idObj)
		}
		jobs, err := model.FindAll(context.TODO(), bson.M{
			"_id": bson.M{
				"$in": idObjs,
			},
			"status": jobsched.SchedJobStatus_SchedJobStatusStopped,
		}, bson.M{"_id": 1})
		if err != nil {
			fastlog.Errorf("err:%v", err)
			continue
		}

		for _, job := range jobs {
			stoppedJobIds = append(stoppedJobIds, job.ID.Hex())
		}
	}

	return
}

func RefreshJobs() (mapGrpToTaskIds map[string][]string) {
	mapGrpToTaskIds = map[string][]string{}

	var pausedJob bool
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		pausedJob = c.PausedJob
	})
	if pausedJob {
		return mapGrpToTaskIds
	}

	model := db.NewJobModel()

	var lastId string
	for {
		filter := bson.M{
			"status": bson.M{"$in": []int32{int32(jobsched.SchedJobStatus_SchedJobStatusNil), int32(jobsched.SchedJobStatus_SchedJobStatusFail)}},
			"next_sched_at": bson.M{
				"$lte": time.Now().Unix(),
			},
		}
		if lastId != "" {
			lastIdObj, err := primitive.ObjectIDFromHex(lastId)
			if err != nil {
				fastlog.Errorf("err:%v", err)
				return
			}
			filter["_id"] = bson.M{"$gt": lastIdObj}
		}
		jobs, err := model.FindMany(context.Background(), filter, bson.D{{"_id", 1}}, 500, bson.M{"_id": 1, "status": 1, "grp_name": 1})
		if err != nil {
			fastlog.Errorf("err:%v", err)
			continue
		}

		if len(jobs) == 0 {
			break
		}

		lastId = jobs[len(jobs)-1].ID.Hex()
		var schedStatusIdObjs []primitive.ObjectID
		for _, job := range jobs {
			jobGrpName := job.GroupName
			if jobGrpName == "" {
				jobGrpName = DefJobGrp
			}
			jobIds := mapGrpToTaskIds[jobGrpName]
			mapGrpToTaskIds[jobGrpName] = append(jobIds, job.ID.Hex())

			if job.Status == int32(jobsched.SchedJobMode_SchedJobModeNil) {
				schedStatusIdObjs = append(schedStatusIdObjs, job.ID)
			}
		}

		if len(schedStatusIdObjs) > 0 {
			now := time.Now()
			updateFilter := bson.M{"_id": bson.M{"$in": schedStatusIdObjs}}
			update := bson.M{
				"$set": bson.M{
					"status":                          jobsched.SchedJobStatus_SchedJobStatusSched,
					"last_sched_at":                   now.Unix(),
					"updated_at":                      now,
					"re_run_count_caz_fail_cur_sched": 0,
				},
				"$inc": bson.M{
					"sched_count": 1,
				},
			}
			fastlog.Infof("update job, filter:%+v update:%+v", updateFilter, update)
			coll, err := model.GetColl()
			if err != nil {
				fastlog.Errorf("err:%v", err)
				continue
			}
			_, err = coll.UpdateMany(context.TODO(), updateFilter, update)
			if err != nil {
				fastlog.Errorf("err:%v", err)
				continue
			}
		}
	}

	lastId = ""
	for {
		filter := bson.M{
			"$or": []bson.M{
				{
					"status": jobsched.SchedJobStatus_SchedJobStatusSched,
					"last_sched_at": bson.M{
						"$lt": time.Now().Unix() - 60,
					},
				},
				{
					"status": jobsched.SchedJobStatus_SchedJobStatusRunning,
					"last_run_at": bson.M{
						"$lt": time.Now().Unix() - 60,
					},
				},
				{
					"status": jobsched.SchedJobStatus_SchedJobStatusRunningAsync,
					"last_heart_beat_in_async_at": bson.M{
						"$lt": time.Now().Unix() - 20,
					},
				},
			},
		}
		if lastId != "" {
			lastIdObj, err := primitive.ObjectIDFromHex(lastId)
			if err != nil {
				fastlog.Errorf("err:%v", err)
				return
			}
			filter["_id"] = bson.M{"$gt": lastIdObj}
		}
		jobs, err := model.FindMany(context.Background(), filter, bson.D{{"_id", 1}}, 500, bson.M{"_id": 1, "grp_name": 1})
		if err != nil {
			fastlog.Errorf("err:%v", err)
			continue
		}

		if len(jobs) == 0 {
			break
		}

		lastId = jobs[len(jobs)-1].ID.Hex()
		var idObjs []primitive.ObjectID
		for _, job := range jobs {
			jobGrpName := job.GroupName
			if jobGrpName == "" {
				jobGrpName = DefJobGrp
			}
			jobIds := mapGrpToTaskIds[jobGrpName]
			mapGrpToTaskIds[jobGrpName] = append(jobIds, job.ID.Hex())
			idObjs = append(idObjs, job.ID)
		}

		now := time.Now()
		updateFilter := bson.M{"_id": bson.M{"$in": idObjs}}
		update := bson.M{
			"$set": bson.M{
				"status":                          jobsched.SchedJobStatus_SchedJobStatusSched,
				"last_sched_at":                   now.Unix(),
				"updated_at":                      now,
				"re_run_count_caz_fail_cur_sched": 0,
			},
			"$inc": bson.M{
				"sched_count":                1,
				"re_sched_count_caz_timeout": 1,
			},
		}
		fastlog.Infof("update job, filter:%+v update:%+v", updateFilter, update)
		coll, err := model.GetColl()
		if err != nil {
			fastlog.Errorf("err:%v", err)
			continue
		}
		_, err = coll.UpdateMany(context.TODO(), updateFilter, update)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			continue
		}
	}

	return mapGrpToTaskIds
}

func NewJob(jobId, _ string) (looptask.Task, error) {
	idObj, err := primitive.ObjectIDFromHex(jobId)
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}
	return &Job{
		Id: idObj,
	}, nil
}
