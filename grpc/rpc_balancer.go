package grpc

import (
	"errors"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/995933447/easymicro/log"
	"github.com/995933447/runtimeutil"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
)

const BalancerNamePrefix = "easymicro"

const (
	BalancerNameNil = ""

	BalancerNameFnvConsistentHash        = BalancerNamePrefix + "_fnv_consistent_hash"
	BalancerNameFnvConsistentHash1aSum32 = BalancerNamePrefix + "_fnv_consistent_hash_32a_sum32"
	BalancerNameFnvConsistentHash1aSum64 = BalancerNamePrefix + "_fnv_consistent_hash_64a_sum64"
	BalancerNameFnvConsistentHash1Sum32  = BalancerNamePrefix + "_fnv_consistent_hash_32_sum32"
	BalancerNameFnvConsistentHash1Sum64  = BalancerNamePrefix + "_fnv_consistent_hash_64_sum64"

	BalancerNameWeightedNode = BalancerNamePrefix + "_weighted_node"
	BalancerNameUserPick     = BalancerNamePrefix + "_user_pick"
	BalancerNameRoundRobin   = BalancerNamePrefix + "_round_robin"
)

var (
	ErrDisabledSpecAddrPicker               = errors.New("disabled spec addr picker")
	ErrNotFoundKeyRPCAddrFromOutgoingCtx    = errors.New("not found key:" + CtxKeyRPCAddr + " from outgoing context metadata")
	ErrNotFoundKeyHashFromOutgoingCtx       = errors.New("not found key:" + CtxKeyRPCHashKey + " from outgoing context metadata")
	ErrParseKeyHashFromOutgoingCtx          = errors.New("parse key:" + CtxKeyRPCHashKey + " from outgoing context metadata failed")
	ErrNotFoundKeyUserSelectFromOutgoingCtx = errors.New("not found key:" + CtxKeyUserSelect + " from outgoing context metadata")
)

func init() {
	balancer.Register(&ReportFullConnStateBalancerBuilder{})

	// ======== 一致性hash轮训器不采用一般的 base.NewBalancerBuilder + balancer.PickerBuilder 的方式注册，因为
	// balancer.PickerBuilder在每个subConn连接ready时候会异步回调一次，不会等待所有subConn连接状态称为ready，也无法读取到
	// 所有连接的地址，做一致性hash取模会出现临时性不准确
	balancer.Register(&ReportFullConnStateBalancerBuilder{
		name:          BalancerNameFnvConsistentHash,
		newPickerFunc: NewFnvConsistentHashingPickerWithConnsFunc(BalancerNameFnvConsistentHash, FnvConsistentHashingPickerBuilderHasher{FnvHashVersionDefault}),
	})

	balancer.Register(&ReportFullConnStateBalancerBuilder{
		name:          BalancerNameFnvConsistentHash1aSum32,
		newPickerFunc: NewFnvConsistentHashingPickerWithConnsFunc(BalancerNameFnvConsistentHash1aSum32, FnvConsistentHashingPickerBuilderHasher{FnvHashVersion1aSum32}),
	})

	balancer.Register(&ReportFullConnStateBalancerBuilder{
		name:          BalancerNameFnvConsistentHash1aSum64,
		newPickerFunc: NewFnvConsistentHashingPickerWithConnsFunc(BalancerNameFnvConsistentHash1aSum64, FnvConsistentHashingPickerBuilderHasher{FnvHashVersion1aSum64}),
	})

	balancer.Register(&ReportFullConnStateBalancerBuilder{
		name:          BalancerNameFnvConsistentHash1Sum32,
		newPickerFunc: NewFnvConsistentHashingPickerWithConnsFunc(BalancerNameFnvConsistentHash1Sum32, FnvConsistentHashingPickerBuilderHasher{FnvHashVersion1Sum32}),
	})

	balancer.Register(&ReportFullConnStateBalancerBuilder{
		name:          BalancerNameFnvConsistentHash1Sum64,
		newPickerFunc: NewFnvConsistentHashingPickerWithConnsFunc(BalancerNameFnvConsistentHash1Sum64, FnvConsistentHashingPickerBuilderHasher{FnvHashVersion1Sum64}),
	})
	// ========

	balancer.Register(base.NewBalancerBuilder(
		BalancerNameWeightedNode,
		&WeightedNodePickerBuilder{
			BalancerName: BalancerNameWeightedNode,
		},
		base.Config{},
	))

	balancer.Register(base.NewBalancerBuilder(
		BalancerNameUserPick,
		&UserPickerBuilder{
			BalancerName: BalancerNameUserPick,
		},
		base.Config{},
	))

	balancer.Register(base.NewBalancerBuilder(
		BalancerNameRoundRobin,
		&RoundRobinPickerBuilder{
			balancerName: BalancerNameRoundRobin,
		},
		base.Config{},
	))
}

var _ balancer.Builder = (*ReportFullConnStateBalancerBuilder)(nil)

type ReportFullConnStateBalancerBuilder struct {
	newPickerFunc func(conns, readyConns map[string]balancer.SubConn) balancer.Picker
	name          string
}

func (b *ReportFullConnStateBalancerBuilder) Name() string {
	return b.name
}

func (b *ReportFullConnStateBalancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	bal := &ReportFullConnStateBalancer{
		cc:            cc,
		conns:         make(map[string]balancer.SubConn),
		readyConns:    make(map[string]balancer.SubConn),
		mapConn2Addr:  make(map[balancer.SubConn]string),
		newPickerFunc: b.newPickerFunc,
	}
	return bal
}

var _ balancer.Balancer = (*ReportFullConnStateBalancer)(nil)

type ReportFullConnStateBalancer struct {
	cc            balancer.ClientConn
	mu            sync.Mutex
	conns         map[string]balancer.SubConn
	mapConn2Addr  map[balancer.SubConn]string
	readyConns    map[string]balancer.SubConn
	newPickerFunc func(conns, readyConns map[string]balancer.SubConn) balancer.Picker
}

func (b *ReportFullConnStateBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 新建 SubConn
	addrSet := make(map[string]struct{})
	for _, addr := range state.ResolverState.Addresses {
		addrSet[addr.Addr] = struct{}{}
		if _, ok := b.conns[addr.Addr]; !ok {
			sc, err := b.cc.NewSubConn([]resolver.Address{addr}, balancer.NewSubConnOptions{})
			if err != nil {
				log.Error(err)
				continue
			}
			b.conns[addr.Addr] = sc
			b.mapConn2Addr[sc] = addr.Addr
			sc.Connect()
		}
	}

	var (
		removeAddrs []string
	)
	for addr := range b.conns {
		if _, ok := addrSet[addr]; ok {
			continue
		}
		removeAddrs = append(removeAddrs, addr)
	}

	for _, addr := range removeAddrs {
		conn := b.conns[addr]
		b.cc.RemoveSubConn(conn)
		delete(b.conns, addr)
		delete(b.readyConns, addr)
		delete(b.mapConn2Addr, conn)
	}

	return nil
}

func (b *ReportFullConnStateBalancer) ResolverError(err error) {
}

func (b *ReportFullConnStateBalancer) UpdateSubConnState(conn balancer.SubConn, state balancer.SubConnState) {
	b.mu.Lock()
	defer b.mu.Unlock()

	addr, ok := b.mapConn2Addr[conn]
	if ok {
		switch state.ConnectivityState {
		case connectivity.Ready:
			b.readyConns[addr] = conn
		case connectivity.Shutdown, connectivity.TransientFailure:
			delete(b.readyConns, addr)
		default:
		}
	}

	// 更新 Picker
	if len(b.readyConns) == 0 {
		b.cc.UpdateState(balancer.State{
			ConnectivityState: connectivity.Connecting, // connecting状态时候,grpc会阻塞直到ConnectivityState变成ready或者超时
			Picker:            b.newPickerFunc(b.conns, b.readyConns),
		})
		return
	}

	// 至少有一个地址链接可用
	b.cc.UpdateState(balancer.State{
		ConnectivityState: connectivity.Ready,
		Picker:            b.newPickerFunc(b.conns, b.readyConns),
	})
}

func (b *ReportFullConnStateBalancer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sc := range b.conns {
		b.cc.RemoveSubConn(sc)
	}
	b.conns = nil
	b.readyConns = nil
	b.mapConn2Addr = nil
}

func (b *ReportFullConnStateBalancer) ExitIdle() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sc := range b.conns {
		sc.Connect()
	}
}

var (
	supportiveSpecAddrPickerWhiteBlackMu   sync.RWMutex
	supportiveSpecAddrPickerBalancerWhites = make(map[string]struct{})
	supportiveSpecAddrPickerBalancerBlacks = make(map[string]struct{})
)

func IsBalancerEnabledSpecAddrPicker(balancerName string) bool {
	supportiveSpecAddrPickerWhiteBlackMu.RLock()
	defer supportiveSpecAddrPickerWhiteBlackMu.RUnlock()

	if len(supportiveSpecAddrPickerBalancerBlacks) > 0 {
		if _, ok := supportiveSpecAddrPickerBalancerWhites[balancerName]; ok {
			return true
		}

		return false
	}

	if len(supportiveSpecAddrPickerBalancerBlacks) > 0 {
		if _, ok := supportiveSpecAddrPickerBalancerBlacks[balancerName]; ok {
			return false
		}

		return true
	}

	return true
}

func SetSupportiveSpecAddrPickerBalancerWhites(balancerNames ...string) {
	supportiveSpecAddrPickerWhiteBlackMu.Lock()
	defer supportiveSpecAddrPickerWhiteBlackMu.Unlock()

	for _, name := range balancerNames {
		supportiveSpecAddrPickerBalancerWhites[name] = struct{}{}
	}
}

func SetSupportiveSpecAddrPickerBalancerBlacks(balancerNames ...string) {
	supportiveSpecAddrPickerWhiteBlackMu.Lock()
	defer supportiveSpecAddrPickerWhiteBlackMu.Unlock()

	for _, name := range balancerNames {
		supportiveSpecAddrPickerBalancerBlacks[name] = struct{}{}
	}
}

func NewSpecAddrPicker(mapAddrToConn map[string]balancer.SubConn, enabled bool) *SpecAddrPicker {
	return &SpecAddrPicker{
		mapAddrToConn: mapAddrToConn,
		enabled:       enabled,
	}
}

var _ balancer.Picker = (*SpecAddrPicker)(nil)

type SpecAddrPicker struct {
	mapAddrToConn map[string]balancer.SubConn
	enabled       bool
}

func (p *SpecAddrPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if !p.enabled {
		return balancer.PickResult{}, ErrDisabledSpecAddrPicker
	}

	meta, ok := metadata.FromOutgoingContext(info.Ctx)
	if !ok {
		return balancer.PickResult{}, ErrNotFoundKeyRPCAddrFromOutgoingCtx
	}

	addresses := meta.Get(CtxKeyRPCAddr)
	if len(addresses) > 0 {
		addr := addresses[0]
		conn, ok := p.mapAddrToConn[addr]
		if !ok {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}

		return balancer.PickResult{
			SubConn: conn,
		}, nil
	}

	return balancer.PickResult{}, ErrNotFoundKeyRPCAddrFromOutgoingCtx
}

type FnvHashVersion int

const (
	FnvHashVersionNil FnvHashVersion = iota
	FnvHashVersion1aSum32
	FnvHashVersion1aSum64
	FnvHashVersion1Sum32
	FnvHashVersion1Sum64
)

const FnvHashVersionDefault = FnvHashVersion1aSum32

func NewFnvConsistentHashingPickerWithConnsFunc(balancerName string, hasher FnvConsistentHashingPickerBuilderHasher) func(conns, readyConns map[string]balancer.SubConn) balancer.Picker {
	return func(conns, readyConns map[string]balancer.SubConn) balancer.Picker {
		var (
			nodes         = make([]*PickerHashNode, 0, len(readyConns))
			mapAddrToConn = make(map[string]balancer.SubConn)
		)
		for addr, conn := range conns {
			hash, err := hasher.hash(addr)
			if err != nil {
				log.Error(runtimeutil.NewStackErr(err))
				continue
			}

			_, ready := readyConns[addr]

			nodes = append(nodes, &PickerHashNode{
				conn:  conn,
				hash:  hash,
				ready: ready,
			})

			if !ready {
				continue
			}

			mapAddrToConn[addr] = conn
		}

		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].hash < nodes[j].hash
		})

		var maxHashNode *PickerHashNode
		if len(nodes) > 0 {
			maxHashNode = nodes[len(nodes)-1]
		}

		return &FnvConsistentHashingPicker{
			SpecAddrPicker:                          NewSpecAddrPicker(mapAddrToConn, IsBalancerEnabledSpecAddrPicker(balancerName)),
			FnvConsistentHashingPickerBuilderHasher: hasher,
			maxHashNode:                             maxHashNode,
			nodes:                                   nodes,
		}
	}
}

var _ balancer.Picker = (*FnvConsistentHashingPicker)(nil)

type PickerHashNode struct {
	conn  balancer.SubConn
	hash  uint64
	ready bool
}

type FnvConsistentHashingPicker struct {
	*SpecAddrPicker
	FnvConsistentHashingPickerBuilderHasher
	maxHashNode *PickerHashNode
	nodes       []*PickerHashNode
}

func (p *FnvConsistentHashingPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	res, err := p.SpecAddrPicker.Pick(info)
	if err != nil {
		if !errors.Is(err, ErrDisabledSpecAddrPicker) && !errors.Is(err, ErrNotFoundKeyRPCAddrFromOutgoingCtx) {
			return balancer.PickResult{}, err
		}
	} else {
		return res, nil
	}

	meta, ok := metadata.FromOutgoingContext(info.Ctx)
	if !ok {
		return balancer.PickResult{}, ErrNotFoundKeyHashFromOutgoingCtx
	}

	hashes := meta.Get(CtxKeyRPCHashKey)
	if len(hashes) == 0 {
		return balancer.PickResult{}, ErrNotFoundKeyHashFromOutgoingCtx
	}

	hashKey := hashes[0]
	hash, err := p.hash(hashKey)
	if err != nil {
		log.Error(runtimeutil.NewStackErr(err))
		return balancer.PickResult{}, ErrParseKeyHashFromOutgoingCtx
	}

	if hash > p.maxHashNode.hash {
		node := p.nodes[0]
		if !node.ready {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable // 返回无地址可用,grpc会阻塞直到有地址可用或者超时
		}

		return balancer.PickResult{
			SubConn: p.nodes[0].conn,
		}, nil
	}

	for _, node := range p.nodes {
		if hash <= node.hash {
			if !node.ready {
				return balancer.PickResult{}, balancer.ErrNoSubConnAvailable // 返回无地址可用,grpc会阻塞直到有地址可用或者超时
			}

			return balancer.PickResult{
				SubConn: node.conn,
			}, nil
		}
	}

	node := p.nodes[0]
	if !node.ready {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable // 返回无地址可用,grpc会阻塞直到有地址可用或者超时
	}

	return balancer.PickResult{
		SubConn: p.nodes[0].conn,
	}, nil
}

var _ base.PickerBuilder = (*FnvConsistentHashingPickerBuilder)(nil)

type FnvConsistentHashingPickerBuilder struct {
	FnvConsistentHashingPickerBuilderHasher
	BalancerName string
}

func (p *FnvConsistentHashingPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	var (
		nodes         = make([]*PickerHashNode, 0, len(info.ReadySCs))
		mapAddrToConn = make(map[string]balancer.SubConn)
	)
	for conn, addr := range info.ReadySCs {
		hash, err := p.hash(addr.Address.Addr)
		if err != nil {
			log.Error(runtimeutil.NewStackErr(err))
			continue
		}
		nodes = append(nodes, &PickerHashNode{
			conn:  conn,
			hash:  hash,
			ready: true,
		})
		mapAddrToConn[addr.Address.Addr] = conn
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].hash < nodes[j].hash
	})

	var maxHashNode *PickerHashNode
	if len(nodes) > 0 {
		maxHashNode = nodes[len(nodes)-1]
	}

	return &FnvConsistentHashingPicker{
		SpecAddrPicker:                          NewSpecAddrPicker(mapAddrToConn, IsBalancerEnabledSpecAddrPicker(p.BalancerName)),
		FnvConsistentHashingPickerBuilderHasher: p.FnvConsistentHashingPickerBuilderHasher,
		maxHashNode:                             maxHashNode,
		nodes:                                   nodes,
	}
}

type FnvConsistentHashingPickerBuilderHasher struct {
	fnvHashVersion FnvHashVersion
}

func (f *FnvConsistentHashingPickerBuilderHasher) hash(addr string) (uint64, error) {
	switch f.fnvHashVersion {
	case FnvHashVersionNil, FnvHashVersion1aSum32:
		h := fnv.New32a()
		_, err := h.Write([]byte(addr))
		if err != nil {
			return 0, err
		}
		return uint64(h.Sum32()), nil
	case FnvHashVersion1aSum64:
		h := fnv.New64a()
		_, err := h.Write([]byte(addr))
		if err != nil {
			return 0, err
		}
		return h.Sum64(), nil
	case FnvHashVersion1Sum32:
		h := fnv.New32()
		_, err := h.Write([]byte(addr))
		if err != nil {
			return 0, err
		}
		return uint64(h.Sum32()), nil
	case FnvHashVersion1Sum64:
		h := fnv.New64()
		_, err := h.Write([]byte(addr))
		if err != nil {
			return 0, err
		}
		return h.Sum64(), nil
	}
	return 0, errors.New("unknown hash version")
}

var ParsePickerNodeWeight = func(addr resolver.Address) (int, error) {
	priorityAny := addr.Attributes.Value("priority")
	switch priority := priorityAny.(type) {
	case int:
		return priority, nil
	case int8:
		return int(priority), nil
	case int32:
		return int(priority), nil
	case int64:
		return int(priority), nil
	case uint8:
		return int(priority), nil
	case uint:
		return int(priority), nil
	case uint32:
		return int(priority), nil
	case uint64:
		return int(priority), nil
	default:
		return 0, errors.New("unknown priority type")
	}
}

var _ balancer.Picker = (*WeightedNodePicker)(nil)

type PickerWeightedNode struct {
	conn   balancer.SubConn
	weight int
}

type WeightedNodePicker struct {
	*SpecAddrPicker
	nodes       []*PickerWeightedNode
	totalWeight int
	rand        *rand.Rand
}

func (p *WeightedNodePicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	res, err := p.SpecAddrPicker.Pick(info)
	if err != nil {
		if !errors.Is(err, ErrDisabledSpecAddrPicker) && !errors.Is(err, ErrNotFoundKeyRPCAddrFromOutgoingCtx) {
			return balancer.PickResult{}, err
		}
	} else {
		return res, nil
	}

	r := p.rand.Intn(p.totalWeight)
	var weight int
	for _, node := range p.nodes {
		weight += node.weight
		if weight > r {
			return balancer.PickResult{
				SubConn: node.conn,
			}, nil
		}
	}

	// technically, would never do it
	r = p.rand.Intn(len(p.nodes))
	return balancer.PickResult{
		SubConn: p.nodes[r].conn,
	}, nil
}

var _ base.PickerBuilder = (*WeightedNodePickerBuilder)(nil)

type WeightedNodePickerBuilder struct {
	BalancerName string
}

func (p *WeightedNodePickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	var (
		nodes         = make([]*PickerWeightedNode, 0, len(info.ReadySCs))
		mapAddrToConn = make(map[string]balancer.SubConn)
		totalWeight   int
	)
	for conn, addr := range info.ReadySCs {
		weight, err := ParsePickerNodeWeight(addr.Address)
		if err != nil {
			log.Error(runtimeutil.NewStackErr(err))
			weight = 1
		}
		nodes = append(nodes, &PickerWeightedNode{
			conn:   conn,
			weight: weight,
		})
		totalWeight += weight
		mapAddrToConn[addr.Address.Addr] = conn
	}
	return &WeightedNodePicker{
		SpecAddrPicker: NewSpecAddrPicker(mapAddrToConn, IsBalancerEnabledSpecAddrPicker(p.BalancerName)),
		nodes:          nodes,
		totalWeight:    totalWeight,
		rand:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

var _ balancer.Picker = (*UserPicker)(nil)

type PickNodeByUserFunc func(info balancer.PickInfo, nodes []*SupportUserPickNode) (balancer.SubConn, error)

var PickNodeByUser PickNodeByUserFunc

var DefaultPickNodeByUserFunc = func(nodeGroupCache map[string][]*SupportUserPickNode, random *rand.Rand) PickNodeByUserFunc {
	return func(info balancer.PickInfo, nodes []*SupportUserPickNode) (balancer.SubConn, error) {
		if nodeGroupCache == nil {
			nodeGroupCache = make(map[string][]*SupportUserPickNode)
		}

		if len(nodeGroupCache) == 0 {
			for _, node := range nodes {
				idAny := node.GetAddress().Attributes.Value("extra")
				id, ok := idAny.(string)
				if !ok {
					continue
				}
				nodeGroup := nodeGroupCache[id]
				nodeGroupCache[id] = append(nodeGroup, node)
			}
		}

		meta, ok := metadata.FromOutgoingContext(info.Ctx)
		if !ok {
			return nil, ErrNotFoundKeyUserSelectFromOutgoingCtx
		}

		userSelects := meta.Get(CtxKeyUserSelect)
		if len(userSelects) == 0 {
			return nil, ErrNotFoundKeyUserSelectFromOutgoingCtx
		}

		nodeGroup, ok := nodeGroupCache[userSelects[0]]
		if !ok {
			return nil, balancer.ErrNoSubConnAvailable
		}

		nodeNum := len(nodeGroup)
		if nodeNum == 0 {
			return nil, balancer.ErrNoSubConnAvailable
		}

		return nodeGroup[random.Intn(nodeNum)].conn, nil
	}
}

type SupportUserPickNode struct {
	conn balancer.SubConn
	addr *resolver.Address
}

func (s *SupportUserPickNode) GetConn() balancer.SubConn {
	return s.conn
}

func (s *SupportUserPickNode) GetAddress() *resolver.Address {
	return s.addr
}

type UserPicker struct {
	*SpecAddrPicker
	nodes          []*SupportUserPickNode
	pickFunc       PickNodeByUserFunc
	nodeGroupCache map[string][]*SupportUserPickNode
	rand           *rand.Rand
}

func (p *UserPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.nodes) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	res, err := p.SpecAddrPicker.Pick(info)
	if err != nil {
		if !errors.Is(err, ErrDisabledSpecAddrPicker) && !errors.Is(err, ErrNotFoundKeyRPCAddrFromOutgoingCtx) {
			return balancer.PickResult{}, err
		}
	} else {
		return res, nil
	}

	conn, err := p.pickFunc(info, p.nodes)
	if err != nil {
		return balancer.PickResult{}, err
	}

	return balancer.PickResult{
		SubConn: conn,
	}, nil
}

var _ base.PickerBuilder = (*UserPickerBuilder)(nil)

type UserPickerBuilder struct {
	BalancerName string
}

func (p *UserPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	var (
		nodes         = make([]*SupportUserPickNode, 0, len(info.ReadySCs))
		mapAddrToConn = make(map[string]balancer.SubConn)
	)
	for conn, addr := range info.ReadySCs {
		address := addr.Address
		node := &SupportUserPickNode{
			conn: conn,
			addr: &address,
		}
		nodes = append(nodes, node)
		mapAddrToConn[address.Addr] = conn
	}

	picker := &UserPicker{
		SpecAddrPicker: NewSpecAddrPicker(mapAddrToConn, IsBalancerEnabledSpecAddrPicker(p.BalancerName)),
		nodes:          nodes,
		nodeGroupCache: make(map[string][]*SupportUserPickNode),
		rand:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	pickFunc := PickNodeByUser
	if pickFunc == nil {
		pickFunc = DefaultPickNodeByUserFunc(picker.nodeGroupCache, picker.rand)
	}

	picker.pickFunc = pickFunc

	return picker
}

var _ balancer.Picker = (*RoundRobinPicker)(nil)

type RoundRobinPicker struct {
	*SpecAddrPicker
	pollCount atomic.Uint64
	conns     []balancer.SubConn
	connLen   uint64
}

func (p *RoundRobinPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if len(p.conns) == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	res, err := p.SpecAddrPicker.Pick(info)
	if err != nil {
		if !errors.Is(err, ErrDisabledSpecAddrPicker) && !errors.Is(err, ErrNotFoundKeyRPCAddrFromOutgoingCtx) {
			return balancer.PickResult{}, err
		}
	} else {
		return res, nil
	}

	return balancer.PickResult{
		SubConn: p.conns[p.pollCount.Add(1)%p.connLen],
	}, nil
}

var _ base.PickerBuilder = (*RoundRobinPickerBuilder)(nil)

type RoundRobinPickerBuilder struct {
	balancerName string
}

func (p *RoundRobinPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	conns := make([]balancer.SubConn, 0, len(info.ReadySCs))
	mapAddrToConn := make(map[string]balancer.SubConn)
	for c, addr := range info.ReadySCs {
		conns = append(conns, c)
		mapAddrToConn[addr.Address.Addr] = c
	}

	return &RoundRobinPicker{
		SpecAddrPicker: NewSpecAddrPicker(mapAddrToConn, IsBalancerEnabledSpecAddrPicker(p.balancerName)),
		conns:          conns,
		connLen:        uint64(len(conns)),
	}
}
