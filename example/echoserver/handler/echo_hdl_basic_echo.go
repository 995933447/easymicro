package handler

import (
	"context"
	"errors"
	"time"

	"github.com/995933447/easymicro/example/echo"
	"github.com/995933447/fastlog"
	"go.mongodb.org/mongo-driver/mongo"
)

func (e *Echo) BasicEcho(ctx context.Context, req *echo.EchoReq) (*echo.EchoResp, error) {
	var resp echo.EchoResp

	mod := echo.NewEchoerModel(int64(time.Now().Weekday()))
	data, err := mod.FindOneByEchoerName(ctx, req.Echo)
	if errors.Is(err, mongo.ErrNoDocuments) {
		data = &echo.EchoerOrm{
			EchoerName: req.Echo,
		}
		err = mod.InsertOne(ctx, data)
		if err != nil {
			fastlog.Error(err)
			return nil, err
		}
	}

	err = (&echo.EchoEvent{Echo: req.Echo}).Send()
	if err != nil {
		fastlog.Error(err)
	}

	resp.Echo = req.Echo

	return &resp, nil
}
