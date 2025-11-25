package handler

import (
    "context"
    "github.com/995933447/easymicro/example/corp"
)

func (s *Corp) GetCorp(ctx context.Context, req *corp.GetCorpReq) (*corp.GetCorpResp, error) {
	var resp corp.GetCorpResp
	return &resp, nil
}