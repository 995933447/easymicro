package jobsched

import (
	"github.com/995933447/natsevent"
)

const SchedJobGroupChangedEventName = "easymicrojob.jobGrpChanged"

type SchedJobGroupChangedEvent struct {
	Name         string `json:"name"`
	CacheVersion uint64 `json:"cache_version"`
}

func (e *SchedJobGroupChangedEvent) Send() error {
	return natsevent.Publish(SchedJobGroupChangedEventName, e)
}
