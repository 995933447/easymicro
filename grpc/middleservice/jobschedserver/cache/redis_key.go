package cache

import (
	"fmt"

	"github.com/995933447/routeredis"
)

func GenJobGroupHashRouteredisKey(name string) *routeredis.Key {
	return routeredis.NewKey(RouteredisKeyRoute, fmt.Sprintf("easymicrojob.group.name:%s", name))
}

func GenSyncCacheToDBTaskZetRedisKey(appId int32) string {
	return fmt.Sprintf("easymicrojob.jobSched.syncCacheToDBTaskZet.appId:%d", appId)
}

func GenSyncCacheToDBTaskAppIdSetRedisKey() string {
	return "easymicrojob.jobSched.syncCacheToDBTaskAppIdSet"
}
