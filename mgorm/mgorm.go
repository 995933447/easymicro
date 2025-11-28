package mgorm

import (
	"time"

	"github.com/995933447/fastlog"
	"github.com/995933447/mgorm"
)

var FastlogMgormQuery mgorm.OnQueryDoneFunc = func(orm *mgorm.Orm, method string, res any, err error, cost time.Duration, args map[string]interface{}) {
	fastlog.PrintInfo(method, "table:"+orm.GetDb()+"."+orm.GetTb(), "args", args, "result", res, "err", err, "cost", cost)
}
