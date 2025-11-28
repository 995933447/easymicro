package grpcgateway

import (
	"context"
	"fmt"

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
		dis, err := manager.GetDiscovery(discoveryName)
		if err != nil {
			return err
		}

		dis.OnSrvUpdated(func(ctx context.Context, evt discovery.Evt, svc *discovery.Service) {
			switch evt {
			case discovery.EvtUpdated:
				leastNode := svc.Nodes[len(svc.Nodes)-1]
				if !leastNode.Available() {
					return
				}
				if err = resolve(leastNode.Host, leastNode.Port); err != nil {
					fastlog.Errorf("err:%v", err)
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

		srvs, err := dis.LoadAll(context.TODO())
		if err != nil {
			fastlog.Errorf("err:%v", err)
			return err
		}

		resolveNodes := make(map[string]*discovery.Node)
		for _, srv := range srvs {
			leastNode := srv.Nodes[len(srv.Nodes)-1]
			resolveNodes[fmt.Sprintf("%s:%d", leastNode.Host, leastNode.Port)] = leastNode
		}

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
}
