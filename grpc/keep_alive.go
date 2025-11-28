package grpc

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// CreateKeepAliveDialOpt 创建客户端保持健康检查长连接的dial option
func CreateKeepAliveDialOpt(heartbeatInterval, heartbeatTimeout time.Duration, keepHeartbeatEvenNoStream bool) grpc.DialOption {
	if heartbeatInterval == 0 {
		heartbeatInterval = time.Second * 10 // 每 10s 发送 ping
	}
	if heartbeatTimeout == 0 {
		heartbeatTimeout = time.Second * 5 // ping 等待 5s 无响应则认为断开
	}
	return grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                heartbeatInterval,
		Timeout:             heartbeatTimeout,
		PermitWithoutStream: keepHeartbeatEvenNoStream,
	})
}

// CreateKeepAliveServerOpt 创建保持服务端向客户端健康检查长连接的server option
func CreateKeepAliveServerOpt(heartbeatInterval, heartbeatTimeout time.Duration) grpc.ServerOption {
	if heartbeatInterval == 0 {
		heartbeatInterval = time.Second * 10 // 服务端 10s 内无活动则 ping 客户端
	}
	if heartbeatTimeout == 0 {
		heartbeatTimeout = time.Second * 5 // ping 等待 5s 无响应则认为断开
	}
	return grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    heartbeatInterval,
		Timeout: heartbeatTimeout,
	})
}

// CreateLimitClientKeepAliveServerOpt 创建限制客户端保持服务端健康检查长连接行为限制的server option
func CreateLimitClientKeepAliveServerOpt(clientHeartbeatMinInterval time.Duration, keepHeartbeatEvenNoStream bool) grpc.ServerOption {
	kaPolicy := keepalive.EnforcementPolicy{
		MinTime:             clientHeartbeatMinInterval, // 客户端 ping 的最小间隔，小于该间隔，服务器认为太频繁，会直接断开连接
		PermitWithoutStream: keepHeartbeatEvenNoStream,  // 如果是 false，客户端在没有rpc的空闲时发 ping 会被断开
	}
	return grpc.KeepaliveEnforcementPolicy(kaPolicy)
}
