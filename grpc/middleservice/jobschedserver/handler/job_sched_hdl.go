package handler

import (
	"github.com/995933447/easymicro/grpc/middleservice/jobsched"
)

type JobSched struct {
	jobsched.UnimplementedJobSchedServer
	ServiceName string
}

var JobSchedHandler = &JobSched{
	ServiceName: jobsched.EasymicroGRPCPbServiceNameJobSched,
}
