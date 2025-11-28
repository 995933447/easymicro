package cache

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
	"github.com/995933447/easymicro/grpc/middleservice/jobschedserver/db"
	"github.com/995933447/fastlog"
	"github.com/995933447/natsevent"
	"github.com/995933447/routeredis"
	"github.com/gomodule/redigo/redis"
	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

const JobGroupLeastVersionRedisField = "objLeastVersionX"

type JobGroupMemCache struct {
	DO       jobsched.JobGroupOrm
	Version  uint64
	ExpireAt time.Time
}

func (j *JobGroupMemCache) Copy() *JobGroupMemCache {
	return &JobGroupMemCache{
		DO:      j.DO,
		Version: j.Version,
	}
}

type JobGroupCacheMgr struct {
	mapNameToGroupMemCache map[string]*JobGroupMemCache
	mapNameToLeastVersion  map[string]uint64
	syncLeastMemCacheCh    chan string
	onJobGroupChanged      func(grpName string)
	mu                     sync.RWMutex
}

var JobGroupCacheManager = &JobGroupCacheMgr{
	mapNameToGroupMemCache: make(map[string]*JobGroupMemCache),
	mapNameToLeastVersion:  make(map[string]uint64),
	syncLeastMemCacheCh:    make(chan string, 100000),
}

func (j *JobGroupCacheMgr) Init() error {
	if err := j.listenJobGroupChanged(); err != nil {
		return err
	}
	j.createSyncLeastMemCacheWorkerPool()
	j.compareAndSyncLeastMemCache()
	j.initExpiredMemCacheClearer()
	return nil
}

func (j *JobGroupCacheMgr) OnJobGroupChanged(fn func(grpName string)) {
	j.onJobGroupChanged = fn
}

func (j *JobGroupCacheMgr) listenJobGroupChanged() error {
	return natsevent.Subscribe(jobsched.SchedJobGroupChangedEventName, jobsched.EasymicroGRPCPbServiceNameJobSched, func(evt *jobsched.SchedJobGroupChangedEvent) error {
		defer func() {
			if j.onJobGroupChanged != nil {
				j.onJobGroupChanged(evt.Name)
			}
		}()

		j.mu.RLock()
		if leastVersion := j.mapNameToLeastVersion[evt.Name]; leastVersion == evt.CacheVersion {
			j.mu.RUnlock()
			return nil
		}
		j.mu.RUnlock()

		j.SetLeastVersion(evt.Name, evt.CacheVersion)
		return nil
	})
}

func (j *JobGroupCacheMgr) GetUpdateBsonFromRedis(name string) (bson.M, bool, error) {
	key := GenJobGroupHashRouteredisKey(name)
	cacheMap, ok, err := routeredis.Hgetall(key)
	if err != nil {
		return nil, false, err
	}

	if !ok || len(cacheMap) == 0 {
		return nil, false, nil
	}

	b := bson.M{}
	entityReflect := reflect.ValueOf(&jobsched.JobGroupOrm{})
	fNum := entityReflect.Elem().NumField()
	for i := 0; i < fNum; i++ {
		fType := entityReflect.Elem().Type().Field(i)
		fName := fType.Name

		mapFieldName := fType.Tag.Get("json")
		mapFieldPos := strings.Index(mapFieldName, ",")
		if mapFieldPos > 0 {
			mapFieldName = mapFieldName[:mapFieldPos]
		}
		if mapFieldName == "" {
			mapFieldName = fName
		}

		valStr, ok := cacheMap[mapFieldName]
		if !ok {
			continue
		}

		bsonFieldName := fType.Tag.Get("bson")
		bsonFieldPos := strings.Index(mapFieldName, ",")
		if bsonFieldPos > 0 {
			bsonFieldName = bsonFieldName[:bsonFieldPos]
		}
		if bsonFieldName == "" {
			bsonFieldName = fName
		}

		if fName == "ID" {
			val, err := primitive.ObjectIDFromHex(valStr)
			if err != nil {
				return nil, false, err
			}

			b[bsonFieldName] = val
		} else {
			fTypeKind := fType.Type.Kind()
			switch fTypeKind {
			case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
				if valStrPos := strings.Index(valStr, "."); valStrPos > 0 {
					valStr = valStr[:valStrPos]
				}
				valInt64, err := strconv.ParseInt(valStr, 10, 64)
				if err != nil {
					return nil, false, err
				}
				switch fTypeKind {
				case reflect.Int:
					b[bsonFieldName] = int(valInt64)
				case reflect.Int8:
					b[bsonFieldName] = int8(valInt64)
				case reflect.Int16:
					b[bsonFieldName] = int16(valInt64)
				case reflect.Int32:
					b[bsonFieldName] = int32(valInt64)
				case reflect.Int64:
					b[bsonFieldName] = valInt64
				}
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if valStrPos := strings.Index(valStr, "."); valStrPos > 0 {
					valStr = valStr[:valStrPos]
				}
				valUint64, err := strconv.ParseUint(valStr, 10, 64)
				if err != nil {
					return nil, false, err
				}
				switch fTypeKind {
				case reflect.Uint:
					b[bsonFieldName] = int(valUint64)
				case reflect.Uint8:
					b[bsonFieldName] = int8(valUint64)
				case reflect.Uint16:
					b[bsonFieldName] = int16(valUint64)
				case reflect.Uint32:
					b[bsonFieldName] = int32(valUint64)
				case reflect.Uint64:
					b[bsonFieldName] = valUint64
				}
			case reflect.Float32, reflect.Float64:
				valFloat64, err := strconv.ParseFloat(valStr, 32)
				if err != nil {
					return nil, false, err
				}
				switch fTypeKind {
				case reflect.Float32:
					b[bsonFieldName] = float32(valFloat64)
				case reflect.Float64:
					b[bsonFieldName] = valFloat64
				}
			default:
				if valStr == "" {
					break
				}

				if fName == "CreatedAt" || fName == "UpdatedAt" {
					val, err := time.Parse("2006-01-02T15:04:05Z", valStr)
					if err != nil {
						return nil, false, err
					}
					b[bsonFieldName] = val
					break
				}

				b[bsonFieldName] = valStr
			}
		}
	}

	return b, true, nil
}

func (j *JobGroupCacheMgr) UpdateWithCache(name string, fields map[string]interface{}) error {
	syncTask := &SyncDBTask{
		TaskType:     SyncDBTaskTypeJobGroup,
		JobGroupName: name,
	}

	// 预写同步任务重做日志，进程异常退出时候恢复使用
	if err := syncCacheToDBSched.PreWriteRedoLog(0, syncTask); err != nil {
		fastlog.Errorf("err:%v", err)
		return err
	}

	fields["updated_at"] = time.Now().Format("2006-01-02T15:04:05.000Z")
	if err := CacheOBJFieldsToRedis(GenJobGroupHashRouteredisKey(name), fields, JobGroupProcVersion, 3600*5, false); err != nil {
		fastlog.Errorf("err:%v", err)
		return err
	}

	if err := j.IncLeastVersionWithRedis(name); err != nil {
		fastlog.Errorf("err:%v", err)
		return err
	}

	// 只有master进行同步
	if !syncCacheToDBSched.IsMaster() {
		return nil
	}

	if err := syncCacheToDBSched.RegMergeTodoTask(0, syncTask); err != nil {
		fastlog.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (j *JobGroupCacheMgr) initExpiredMemCacheClearer() {
	go func() {
		for {
			time.Sleep(time.Second)

			j.mu.Lock()
			for name, memCache := range j.mapNameToGroupMemCache {
				if memCache.ExpireAt.After(time.Now()) {
					continue
				}
				delete(j.mapNameToGroupMemCache, name)
				delete(j.mapNameToLeastVersion, name)
			}
			j.mu.Unlock()
		}
	}()
}

func (j *JobGroupCacheMgr) compareAndSyncLeastMemCache() {
	go func() {
		for {
			time.Sleep(time.Second)

			var syncJobGroupNames []string

			j.mu.RLock()
			for name, leastVersion := range j.mapNameToLeastVersion {
				if memCache, ok := j.mapNameToGroupMemCache[name]; !ok || memCache.Version != leastVersion {
					syncJobGroupNames = append(syncJobGroupNames, name)
				}
			}
			j.mu.RUnlock()

			for _, name := range syncJobGroupNames {
				j.syncLeastMemCacheCh <- name
			}
		}
	}()
}

func (j *JobGroupCacheMgr) createSyncLeastMemCacheWorkerPool() {
	for i := 0; i < 3; i++ {
		go func() {
			for {
				name := <-j.syncLeastMemCacheCh
				dataObj, version, exists, err := j.QryFromRedis(name)
				if err != nil {
					fastlog.Errorf("err:%v", err)
					continue
				}

				if !exists {
					continue
				}

				redisKey := GenJobGroupHashRouteredisKey(name)
				ttl, err := redis.Int(routeredis.DoCmdWithTTL(nil, "TTL", redisKey))
				if err != nil {
					fastlog.Errorf("err:%v", err)
					continue
				}

				// 过期了
				if ttl < 0 {
					continue
				}

				func() {
					j.mu.Lock()
					defer j.mu.Unlock()
					// 版本过期
					if leastVersion, ok := j.mapNameToLeastVersion[name]; !ok || leastVersion != version {
						return
					}
					j.mapNameToGroupMemCache[name] = &JobGroupMemCache{
						DO:       *dataObj,
						ExpireAt: time.Now().Add(time.Duration(ttl-5) * time.Second), // 内存提前5s过期
						Version:  version,
					}
				}()
			}
		}()
	}
}

func (j *JobGroupCacheMgr) QryFromRedis(name string) (*jobsched.JobGroupOrm, uint64, bool, error) {
	cacheMap, ok, err := routeredis.Hgetall(GenJobGroupHashRouteredisKey(name))
	if err != nil {
		return nil, 0, false, err
	}

	if len(cacheMap) == 0 || !ok {
		return nil, 0, false, nil
	}

	if val, ok := cacheMap[IsWholeEntityRedisField]; !ok || val != JobGroupProcVersion {
		return nil, 0, false, nil
	}

	var jobGroup jobsched.JobGroupOrm
	entityReflect := reflect.ValueOf(&jobGroup)
	fNum := entityReflect.Elem().NumField()
	for i := 0; i < fNum; i++ {
		fType := entityReflect.Elem().Type().Field(i)
		fName := fType.Name
		mapFieldName := fType.Tag.Get("json")
		mapFieldPos := strings.Index(mapFieldName, ",")
		if mapFieldPos > 0 {
			mapFieldName = mapFieldName[:mapFieldPos]
		}
		if mapFieldName == "" {
			mapFieldName = fName
		}
		valStr, ok := cacheMap[mapFieldName]
		if !ok {
			continue
		}

		if fName == "ID" {
			val, err := primitive.ObjectIDFromHex(valStr)
			if err != nil {
				return nil, 0, false, err
			}
			entityReflect.Elem().FieldByName(fName).Set(reflect.ValueOf(val))
		} else {
			switch fType.Type.Kind() {
			case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64:
				if valStrPos := strings.Index(valStr, "."); valStrPos > 0 {
					valStr = valStr[:valStrPos]
				}
				valInt64, err := strconv.ParseInt(valStr, 10, 64)
				if err != nil {
					return nil, 0, false, err
				}
				entityReflect.Elem().FieldByName(fName).SetInt(valInt64)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				if valStrPos := strings.Index(valStr, "."); valStrPos > 0 {
					valStr = valStr[:valStrPos]
				}
				valUint64, err := strconv.ParseUint(valStr, 10, 64)
				if err != nil {
					return nil, 0, false, err
				}
				entityReflect.Elem().FieldByName(fName).SetUint(valUint64)
			case reflect.Float32, reflect.Float64:
				valFloat, err := strconv.ParseFloat(valStr, 32)
				if err != nil {
					return nil, 0, false, err
				}
				entityReflect.Elem().FieldByName(fName).SetFloat(valFloat)
			default:
				if valStr == "" {
					break
				}

				if fName == "CreatedAt" || fName == "UpdatedAt" {
					val, err := time.Parse("2006-01-02T15:04:05Z", valStr)
					if err != nil {
						return nil, 0, false, err
					}
					entityReflect.Elem().FieldByName(fName).Set(reflect.ValueOf(val))
					break
				}

				entityReflect.Elem().FieldByName(fName).Set(reflect.ValueOf(valStr))
			}
		}
	}

	versionStr := cacheMap[JobGroupLeastVersionRedisField]
	var version uint64
	if versionStr != "" {
		var err error
		version, err = strconv.ParseUint(versionStr, 10, 64)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return nil, 0, false, err
		}
	}

	return &jobGroup, version, true, nil
}

func (j *JobGroupCacheMgr) QryWithRedis(name string) (*jobsched.JobGroupOrm, bool, error) {
	// redis查询
	jobGroup, version, exists, err := j.QryFromRedis(name)
	if err != nil {
		fastlog.Importantf("err:%v", err)
		return nil, false, err
	}

	j.SetLeastVersion(name, version)

	if exists {
		return jobGroup, true, nil
	}

	// redis不存在
	jobGroup, err = db.NewJobGroupModel().FindOne(context.Background(), bson.M{
		"name": name,
	})
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			fastlog.Importantf("err:%v", err)
			return nil, false, err
		}
		return nil, false, nil
	}

	jobGroupMap := map[string]interface{}{}
	js, err := jsoniter.MarshalToString(jobGroup)
	if err != nil {
		fastlog.Importantf("err:%v", err)
		return nil, false, err
	}

	err = jsoniter.UnmarshalFromString(js, &jobGroupMap)
	if err != nil {
		fastlog.Importantf("err:%v", err)
		return nil, false, err
	}

	go func() {
		err = CacheOBJFieldsToRedis(GenJobGroupHashRouteredisKey(name), jobGroupMap, JobGroupProcVersion, 3600, true)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return
		}

		err = j.IncLeastVersionWithRedis(name)
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return
		}
	}()

	return jobGroup, true, nil
}

func (j *JobGroupCacheMgr) IncLeastVersionWithRedis(name string) error {
	leastVersion, err := routeredis.Hincrby(GenJobGroupHashRouteredisKey(name), JobGroupLeastVersionRedisField, 1, 0)
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return err
	}
	j.SetLeastVersion(name, uint64(leastVersion))
	go (&jobsched.SchedJobGroupChangedEvent{Name: name, CacheVersion: uint64(leastVersion)}).Send()
	return nil
}

func (j *JobGroupCacheMgr) QryWithCache(name string) (*jobsched.JobGroupOrm, bool, error) {
	j.mu.Lock()
	// 内存缓存有
	jobGroup, ok := j.mapNameToGroupMemCache[name]
	if ok {
		// 缓存最新版本
		leastVersion := j.mapNameToLeastVersion[name]
		// 还没过期
		if jobGroup.ExpireAt.After(time.Now()) {
			// 最新版本号一致
			if jobGroup.Version == leastVersion {
				j.mu.Unlock()
				return &jobGroup.DO, true, nil
			}
		} else {
			// 过期了
			delete(j.mapNameToGroupMemCache, name)
			delete(j.mapNameToLeastVersion, name)
		}
	}
	j.mu.Unlock()

	// 从redis读取最新数据
	dataObj, exists, err := j.QryWithRedis(name)
	if err != nil {
		fastlog.Importantf("err:%v", err)
		return nil, false, err
	}

	if !exists {
		return nil, false, nil
	}

	return dataObj, true, nil
}

func (j *JobGroupCacheMgr) SetLeastVersion(name string, version uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	old := j.mapNameToLeastVersion[name]
	if version <= old {
		return
	}
	j.mapNameToLeastVersion[name] = version
}
