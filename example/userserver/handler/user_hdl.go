package handler

import (
	"github.com/995933447/easymicro/example/user"
)

type User struct {
	user.UnimplementedUserServer
	ServiceName string
}

var UserHandler = &User{
	ServiceName: user.EasymicroGRPCPbServiceNameUser,
}
