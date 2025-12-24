package grpcgateway

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/995933447/discovery"
	"github.com/995933447/discovery/manager"
	"github.com/995933447/easymicro/grpc"
	"github.com/995933447/fastlog"
	"github.com/995933447/grpcgateway"
	"github.com/995933447/runtimeutil"
)

func InitGRPCResolverFunc(ctx context.Context, resolveSchema, discoveryName string) func() error {
	return func() error {
		return grpc.PrepareDiscoverGRPC(ctx, resolveSchema, discoveryName)
	}
}

func InitAndWatchGRPCClientMetadataFunc(discoveryName string) func(resolve func(svcHost string, svcPort int) error) error {
	return func(resolve func(svcHost string, svcPort int) error) error {
		var mu sync.Mutex

		dis, err := manager.GetDiscovery(discoveryName)
		if err != nil {
			return err
		}

		dis.OnSrvUpdated(func(ctx context.Context, evt discovery.Evt, svc *discovery.Service) {
			switch evt {
			case discovery.EvtUpdated:
				for _, node := range svc.Nodes {
					if !node.Available() {
						continue
					}

					mu.Lock()
					if err = resolve(node.Host, node.Port); err != nil {
						fastlog.Errorf("err:%v", err)
					}
					mu.Unlock()
				}
			case discovery.EvtDeleted:
				var rmKeys []string
				grpcgateway.WalkRpcMetadata(func(key string, meta *grpcgateway.RpcMetadata) bool {
					if meta.GetServiceFullyQualifiedName() == svc.SrvName {
						rmKeys = append(rmKeys, key)
					}
					return true
				})
				for _, key := range rmKeys {
					grpcgateway.DeleteRpcMetadata(key)
				}
			default:
			}
		})

		resolver := func() error {
			srvs, err := dis.LoadAll(context.TODO())
			if err != nil {
				fastlog.Errorf("err:%v", err)
				return err
			}

			resolveNodes := make(map[string]*discovery.Node)
			for _, srv := range srvs {
				for _, node := range srv.Nodes {
					if !node.Available() {
						continue
					}

					resolveNodes[fmt.Sprintf("%s:%d", node.Host, node.Port)] = node
				}
			}

			mu.Lock()
			defer mu.Unlock()

			eg := runtimeutil.NewErrGrp()
			for _, node := range resolveNodes {
				leastNode := node
				eg.Go(func() error {
					if err = resolve(leastNode.Host, leastNode.Port); err != nil {
						fastlog.Errorf("err:%v", err)
						return err
					}

					return nil
				})
			}
			if err = eg.Wait(); err != nil {
				fastlog.Errorf("err:%v", err)
			}

			return nil
		}

		if err := resolver(); err != nil {
			fastlog.Errorf("err:%v", err)
			return err
		}

		go func() {
			// 定时主动刷新服务节点,兜底逻辑
			for {
				time.Sleep(time.Minute * 10)
				if err := resolver(); err != nil {
					fastlog.Errorf("err:%v", err)
				}
			}
		}()

		return nil
	}
}
