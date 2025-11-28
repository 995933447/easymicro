package handler

import (
	"context"

	"github.com/995933447/easymicro/example/echo"
	"github.com/995933447/easymicro/example/user"
	"github.com/995933447/easymicro/grpc"
)

func (s *User) GetUserInfo(ctx context.Context, req *user.GetUserInfoReq) (*user.GetUserInfoResp, error) {
	//var resp user.GetUserInfoResp
	//return &resp, nil
	return nil, grpc.NewRPCErr(echo.ErrCode_ErrFail)
}
