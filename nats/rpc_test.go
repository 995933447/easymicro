package nats

import (
	"context"
	"testing"
	"time"

	"github.com/995933447/easymicro/grpc/middleservice/healthreporter"
	"github.com/995933447/easymicro/loader"
)

func TestRPC(t *testing.T) {
	if err := loader.LoadNatsConfigFromLocal(); err != nil {
		panic(err)
	}

	err := HandleLikeGRPC(healthreporter.HealthReporter_ServiceDesc.ServiceName, "Ping", healthreporter.NewReporter([]string{"test"}).Ping, func() *healthreporter.PingReq {
		return &healthreporter.PingReq{}
	})
	if err != nil {
		t.Error(err)
	}

	var resp healthreporter.PingResp
	err = CallLikeGRPC(context.TODO(), healthreporter.HealthReporter_ServiceDesc.ServiceName, "Ping", &healthreporter.PingReq{PingService: "test"}, &resp, time.Second)
	if err != nil {
		t.Error(err)
	}

	select {}
}
