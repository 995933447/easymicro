package handler

import (
	"github.com/995933447/easymicro/example/corp"
)

type Corp struct {
	corp.UnimplementedCorpServer
	ServiceName string
}

var CorpHandler = &Corp{
	ServiceName: corp.EasymicroGRPCPbServiceNameCorp,
}