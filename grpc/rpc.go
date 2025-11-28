package grpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	discovergrpc "github.com/995933447/discovery/grpc"
	"github.com/995933447/discovery/manager"
	"github.com/995933447/easymicro/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
)

const CtxKeyPrefix = "easymicro"
const DefaultGRPCResolveScheme = "easymicro"

const (
	CtxKeyTrace      = CtxKeyPrefix + "_trace"
	CtxKeyRPCHashKey = CtxKeyPrefix + "_grpc_hash_key"
	CtxKeyRPCAddr    = CtxKeyPrefix + "_grpc_addr"
	CtxKeyUserSelect = CtxKeyPrefix + "_user_select"
)

var (
	globalDialOpts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingPolicy": "%s"}`, BalancerNameRoundRobin)),
	}
	globalDialOptsVersion atomic.Uint64
	globalDialOptsMu      sync.RWMutex
)

func RegisterGlobalDialOpts(opts ...grpc.DialOption) {
	globalDialOptsMu.Lock()
	defer globalDialOptsMu.Unlock()
	globalDialOptsVersion.Add(1)
	globalDialOpts = nil
	globalDialOpts = append(globalDialOpts, opts...)
}

func GetGlobalDialOpts() []grpc.DialOption {
	globalDialOptsMu.RLock()
	defer globalDialOptsMu.RUnlock()
	return globalDialOpts
}

type ServiceDialOptsBuilder struct {
	opts                    []grpc.DialOption
	mu                      sync.RWMutex
	shouldMergeGlobal       bool
	cachedMergedOpts        []grpc.DialOption
	cachedGlobalOptsVersion uint64
}

var serviceOptsBuilders sync.Map

func RegisterServiceDialOpts(serviceName string, shouldMergeGlobal bool, opts ...grpc.DialOption) {
	serviceOptsBuilders.Store(serviceName, &ServiceDialOptsBuilder{
		opts:              opts,
		shouldMergeGlobal: shouldMergeGlobal,
	})
}

func BuildServiceDialOpts(serviceName string) []grpc.DialOption {
	builderAny, ok := serviceOptsBuilders.Load(serviceName)
	if !ok {
		return GetGlobalDialOpts()
	}

	builder := builderAny.(*ServiceDialOptsBuilder)
	if !builder.shouldMergeGlobal && len(builder.opts) == 0 {
		return builder.opts
	}

	builder.mu.RLock()
	if len(builder.cachedMergedOpts) > 0 && builder.cachedGlobalOptsVersion == globalDialOptsVersion.Load() {
		builder.mu.RUnlock()
		return builder.cachedMergedOpts
	}
	builder.mu.RUnlock()

	builder.mu.Lock()
	defer builder.mu.Unlock()

	if len(builder.cachedMergedOpts) > 0 && builder.cachedGlobalOptsVersion == globalDialOptsVersion.Load() {
		return builder.cachedMergedOpts
	}

	builder.cachedMergedOpts = append(GetGlobalDialOpts(), builder.opts...)
	builder.cachedGlobalOptsVersion = globalDialOptsVersion.Load()
	return builder.cachedMergedOpts
}

func PrepareDiscoverGRPC(ctx context.Context, resolveSchema, discoveryName string) error {
	if discoveryName == "" {
		discoveryName = manager.DefaultDiscoveryName
	}

	if resolveSchema == "" {
		resolveSchema = DefaultGRPCResolveScheme
	}

	dis, err := manager.GetDiscovery(discoveryName)
	if err != nil {
		return err
	}

	builder, err := discovergrpc.NewBuilder(ctx, &discovergrpc.BuilderOptions{
		ResolveSchema: resolveSchema,
		Discovery:     dis,
		LogErrorFunc: func(err any) {
			log.Error(err)
		},
	})
	if err != nil {
		return err
	}

	resolver.Register(builder)

	return nil
}
