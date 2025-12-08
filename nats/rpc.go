package nats

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/log"
	"github.com/995933447/easymicro/node"
	"github.com/995933447/natsevent"
	"github.com/995933447/runtimeutil"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const ConnNameRPC = natsevent.ConnNameDefault

type handleLikeGRPCResp struct {
	ErrCode uint32
	ErrMsg  string
	Data    []byte
}

type handleLikeGRPCReq struct {
	Data []byte
	MD   metadata.MD
}

func HandleLikeGRPC[RQ proto.Message, RP proto.Message](serviceName string, method string, fn func(context.Context, RQ) (RP, error), newRQ func() RQ) error {
	host, err := node.GetOrAutoSetHost()
	if err != nil {
		return err
	}

	port, err := node.GetOrAutoSetPort()
	if err != nil {
		return err
	}

	conn, ok := natsevent.GetConn(ConnNameRPC)
	if !ok {
		return errors.New("nats connection:default not found")
	}

	var serviceNames []string
	serviceNames = append(serviceNames, serviceName, fmt.Sprintf("%s_%s:%d", serviceName, host, port))
	for _, name := range serviceNames {
		subject := name + "." + method
		_, err = conn.QueueSubscribe(subject, name, func(msg *nats.Msg) {
			var handleRPCReq handleLikeGRPCReq
			if err := runtimeutil.ForceNumberJson.Unmarshal(msg.Data, &handleRPCReq); err != nil {
				log.Errorf("json.Unmarshal err:%v", err)
				return
			}

			if handleRPCReq.MD == nil {
				handleRPCReq.MD = metadata.MD{}
			}

			if traces := handleRPCReq.MD.Get(grpc.CtxKeyTrace); len(traces) > 0 {
				trace := traces[0]
				runtimeutil.StoreTrace(trace)
				defer runtimeutil.AutoRemoveTrace()
			}

			req := newRQ()
			if err = proto.Unmarshal(handleRPCReq.Data, req); err != nil {
				log.Errorf("proto.Unmarshal err:%v", err)
				return
			}

			runtimeutil.Go(func() {
				defer func() {
					if r := recover(); r != nil {
						// 打印错误
						log.Panicf("[PANIC RECOVER] method=%s panic=%v req:%+v", method, r, req)
						// 堆栈
						log.Panicf("[STACK] %s", debug.Stack())
						// 返回 gRPC 错误
						err = status.Errorf(codes.Internal, "server panic: %v", r)
					}
				}()

				resp, fne := fn(metadata.NewIncomingContext(context.Background(), handleRPCReq.MD), req)

				var handleRPCResp handleLikeGRPCResp

				if fne != nil {
					if st, ok := status.FromError(fne); ok {
						handleRPCResp.ErrMsg = st.Message()
						handleRPCResp.ErrCode = uint32(st.Code())
					} else {
						handleRPCResp.ErrCode = uint32(codes.Unknown)
						handleRPCResp.ErrMsg = fne.Error()
					}
				}

				if any(resp) != nil {
					handleRPCResp.Data, err = proto.Marshal(resp)
					if err != nil {
						log.Errorf("proto.Marshal err:%v", err)
						return
					}
				}

				j, err := jsoniter.Marshal(&handleRPCResp)
				if err != nil {
					log.Errorf("json.Marshal err:%v", err)
					return
				}

				if err = msg.Respond(j); err != nil {
					log.Errorf("msg.Respond err:%v", err)
				}
			})
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func CallLikeGRPC(ctx context.Context, serviceName, method string, req proto.Message, resp proto.Message, timeout time.Duration) error {
	conn, ok := natsevent.GetConn(ConnNameRPC)
	if !ok {
		return errors.New("nats connection:default not found")
	}

	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	md, _ := metadata.FromOutgoingContext(ctx)

	natsReq := &handleLikeGRPCReq{
		Data: data,
		MD:   md,
	}

	j, err := jsoniter.Marshal(natsReq)
	if err != nil {
		return err
	}

	subject := serviceName + "." + method
	if md != nil {
		if addresses := md.Get(grpc.CtxKeyRPCAddr); len(addresses) > 0 {
			addr := addresses[0]
			subject = fmt.Sprintf("%s_%s.%s", serviceName, addr, method)
		}
	}
	reply, err := conn.Request(subject, j, timeout)
	if err != nil {
		return err
	}

	var natsResp handleLikeGRPCResp
	if err = jsoniter.Unmarshal(reply.Data, &natsResp); err != nil {
		return err
	}

	if natsResp.ErrCode != 0 {
		return status.New(codes.Code(natsResp.ErrCode), natsResp.ErrMsg).Err()
	}

	if err = proto.Unmarshal(natsResp.Data, resp); err != nil {
		return err
	}

	return nil
}
