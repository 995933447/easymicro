package cache

import (
	"fmt"

	"github.com/995933447/fastlog"
	"github.com/995933447/routeredis"
)

const IsWholeEntityRedisField = "isWholeEntityX"
const RouteredisKeyRoute = "default"

func CacheOBJFieldsToRedis(key *routeredis.Key, fields map[string]interface{}, procVersion string, ttl int64, isWholeEntity bool) error {
	if isWholeEntity {
		for k, v := range fields {
			switch v.(type) {
			case float32, float64:
				_, err := routeredis.Hsetnx(key, k, fmt.Sprintf("%f", v), ttl)
				if err != nil {
					fastlog.Error(err)
					return err
				}
			default:
				_, err := routeredis.Hsetnx(key, k, v, ttl)
				if err != nil {
					fastlog.Error(err)
					return err
				}
			}
		}
		if _, err := routeredis.Hsetnx(key, IsWholeEntityRedisField, procVersion, ttl); err != nil {
			fastlog.Errorf("err:%v", err)
			return err
		}

		return nil
	}

	var hMSet []interface{}
	for k, v := range fields {
		switch v.(type) {
		case float32, float64:
			hMSet = append(hMSet, k, fmt.Sprintf("%f", v))
		default:
			hMSet = append(hMSet, k, v)
		}
	}

	err := routeredis.Hmset(ttl, key, hMSet...)
	if err != nil {
		fastlog.Errorf("err:%v", err)
		return err
	}

	return nil
}

func NewRouteredisKey(key string) *routeredis.Key {
	return routeredis.NewKey(RouteredisKeyRoute, key)
}
