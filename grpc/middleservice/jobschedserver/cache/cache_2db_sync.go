package cache

import (
	"context"
	"errors"
	"fmt"

	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/config"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"github.com/995933447/mergetodotask"
	"github.com/995933447/routeredis"
	jsoniter "github.com/json-iterator/go"
)

func DecodeSyncDBTask(taskStr string) (mergetodotask.MergeTodoTask, error) {
	var syncDBTask SyncDBTask
	if err := jsoniter.UnmarshalFromString(taskStr, &syncDBTask); err != nil {
		fastlog.Errorf("err:%v", err)
		return nil, err
	}
	return &syncDBTask, nil
}

const (
	SyncDBTaskTypeNil      = 0
	SyncDBTaskTypeJobGroup = 1
)

type SyncDBTask struct {
	TaskType     int    `json:"task_type"`
	JobGroupName string `json:"job_group_name"`
}

func (s *SyncDBTask) GetBucketId() int64 {
	return 0
}

func (s *SyncDBTask) String() string {
	return fmt.Sprintf("{\"task_type\":%d,\"job_group_name\":\"%s\"}", s.TaskType, s.JobGroupName)
}

var _ mergetodotask.MergeTodoTask = (*SyncDBTask)(nil)

var syncCacheToDBSched *mergetodotask.MergeTodoTaskSched

func InitSyncCacheToDBSched() error {
	var err error

	var redisConnName string
	config.SafeReadServerConfig(func(c *config.ServerConfig) {
		redisConnName = c.GetRedisConn()
	})
	redisPool, err := routeredis.NewDynamicConnPool(redisConnName)
	if err != nil {
		return err
	}

	syncCacheToDBSched, err = mergetodotask.NewMergeTodoTaskSched(&mergetodotask.MergeTodoTaskSchedConf{
		Name:                          jobsched.EasymicroGRPCPbServiceNameJobSched + "_mergeTodoTask",
		ConcurWorkerNum:               32,
		TaskAppIdSetRedisKey:          GenSyncCacheToDBTaskAppIdSetRedisKey(),
		GenTaskSetRedisKeyHdl:         GenSyncCacheToDBTaskZetRedisKey,
		DecodeMergeTodoTaskFromString: DecodeSyncDBTask,
		OnExecTask:                    onExecCacheSyncDB,
		RedisPool:                     redisPool,
	})
	if err != nil {
		fastlog.Errorf("err:%v", err)
	}
	return err
}

func onExecCacheSyncDB(task mergetodotask.MergeTodoTask) error {
	specTask := task.(*SyncDBTask)
	switch specTask.TaskType {
	case SyncDBTaskTypeJobGroup:
		updateBson, ok, err := JobGroupCacheManager.GetUpdateBsonFromRedis(specTask.JobGroupName)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return err
		}
		if !ok {
			return nil
		}
		_, err = db.NewJobGroupModel().UpdateOneByName(context.Background(), specTask.JobGroupName, updateBson)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return err
		}
	default:
		return errors.New("invalid task type")
	}
	return nil
}
