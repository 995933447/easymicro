package handler

import (
    "context"
    "github.com/995933447/easymicro/example/user"
)

func (s *User) GetUserInfo(ctx context.Context, req *user.GetUserInfoReq) (*user.GetUserInfoResp, error) {
	var resp user.GetUserInfoResp
	return &resp, nil
}