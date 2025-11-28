package handler

import (
	"github.com/995933447/easymicro/grpc/middleservice/healthchecker"
)

type HealthChecker struct {
	healthchecker.UnimplementedHealthCheckerServer
	ServiceName string
}

var HealthCheckerHandler = &HealthChecker{
	ServiceName: healthchecker.EasymicroGRPCPbServiceNameHealthChecker,
}