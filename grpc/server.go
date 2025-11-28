package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/995933447/discovery"
	"github.com/995933447/discovery/manager"
	"github.com/995933447/easymicro/grpc/middleservice/healthreporter"
	"github.com/995933447/easymicro/log"
	nodemeta "github.com/995933447/easymicro/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ServeGRPCOptions struct {
	DiscoveryName               string
	ServiceNames                []string
	Host                        string
	Port                        int
	RegisterServiceServersFunc  func(*grpc.Server) error
	BeforeRegisterNodeDiscovery func(*discovery.Node) error
	OnRunServer                 func(*grpc.Server, *discovery.Node)
	EnabledHealth               bool
	GRPCServerOpts              []grpc.ServerOption
	StopCtx                     context.Context
	GracefulStopCtx             context.Context
}

func ServeGRPC(ctx context.Context, opts *ServeGRPCOptions) error {
	var err error

	host := opts.Host
	if host == "" {
		host, err = nodemeta.GetOrAutoSetHost()
		if err != nil {
			return err
		}
	}

	port := opts.Port
	if port == 0 {
		port, err = nodemeta.GetOrAutoSetPort()
		if err != nil {
			return err
		}
	}

	node := discovery.NewNode(host, port)
	grpcServer := grpc.NewServer(opts.GRPCServerOpts...)
	if opts.RegisterServiceServersFunc != nil {
		if err = opts.RegisterServiceServersFunc(grpcServer); err != nil {
			return err
		}
	}

	reflection.Register(grpcServer)

	if opts.EnabledHealth {
		healthreporter.RegisterHealthReporterServer(grpcServer, healthreporter.NewReporter(opts.ServiceNames))
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}

	var dis discovery.Discovery
	if opts.DiscoveryName == "" {
		dis, err = manager.GetDefaultDiscovery()
		if err != nil {
			return err
		}
	} else {
		dis, err = manager.GetDiscovery(opts.DiscoveryName)
		if err != nil {
			return err
		}
	}

	defer func() {
		for _, serviceName := range opts.ServiceNames {
			err = dis.Unregister(ctx, serviceName, node, true)
			if err != nil {
				log.Errorf("discovery unregister err: %v", err)
			}
		}
	}()

	for _, serviceName := range opts.ServiceNames {
		if opts.BeforeRegisterNodeDiscovery != nil {
			if err = opts.BeforeRegisterNodeDiscovery(node); err != nil {
				return err
			}
		}

		err = dis.Register(ctx, serviceName, node)
		if err != nil {
			return err
		}
	}

	if opts.OnRunServer != nil {
		opts.OnRunServer(grpcServer, node)
	}

	if opts.StopCtx != nil || opts.GracefulStopCtx != nil {
		go func() {
			if opts.StopCtx != nil && opts.GracefulStopCtx != nil {
				select {
				case <-opts.StopCtx.Done():
					grpcServer.Stop()
				case <-opts.GracefulStopCtx.Done():
					grpcServer.GracefulStop()
				}
			} else if opts.StopCtx != nil {
				<-opts.StopCtx.Done()
				grpcServer.Stop()
			} else {
				<-opts.GracefulStopCtx.Done()
				grpcServer.GracefulStop()
			}
		}()
	}

	err = grpcServer.Serve(listener)
	if err != nil {
		return err
	}

	return nil
}
