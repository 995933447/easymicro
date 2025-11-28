package handler

import (
	"github.com/995933447/easymicro/example/echo"
)

type Echo struct {
	echo.UnimplementedEchoServer
	ServiceName string
}

var EchoHandler = &Echo{
	ServiceName: echo.EasymicroGRPCPbServiceNameEcho,
}