package healthchecker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/995933447/discovery"
	easymicrogrpc "github.com/995933447/easymicro/grpc"
	"github.com/995933447/easymicro/grpc/middleservice/healthreporter"
	"github.com/995933447/easymicro/log"
	"github.com/995933447/runtimeutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var nodeFailSinceMap sync.Map

func saveNodeFailSince(node *Node) {
	now := time.Now()
	nodeFailSinceMap.LoadOrStore(getNodeFailSinceKey(node), &now)
}

func getNodeFailSince(node *Node) (*time.Time, bool) {
	t, ok := nodeFailSinceMap.Load(getNodeFailSinceKey(node))
	return t.(*time.Time), ok
}

func removeNodeFailSince(node *Node) {
	nodeFailSinceMap.Delete(getNodeFailSinceKey(node))
}

func getNodeFailSinceKey(node *Node) string {
	return fmt.Sprintf("%s.%s.%d", node.srvName, node.detail.Host, node.detail.Port)
}

const checkWorkerPoolSize = 100

type Node struct {
	srvName string
	detail  *discovery.Node
}

type CheckerOptions struct {
	Discovery                  discovery.Discovery
	CheckWorkerPoolSize        uint32
	CheckIntervalMs            uint32
	GRPCDailOpts               []grpc.DialOption
	HardDeleteNodeOverFailTime time.Duration
}

func NewChecker(opts *CheckerOptions) *Checker {
	if len(opts.GRPCDailOpts) == 0 {
		opts.GRPCDailOpts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	if opts.CheckIntervalMs == 0 {
		opts.CheckIntervalMs = 5000
	}
	return &Checker{
		Discovery:                  opts.Discovery,
		checkWorkerPoolSize:        checkWorkerPoolSize,
		checkIntervalMs:            opts.CheckIntervalMs,
		grpcDialOpts:               opts.GRPCDailOpts,
		hardDeleteNodeOverFailTime: opts.HardDeleteNodeOverFailTime,
	}
}

type Checker struct {
	discovery.Discovery
	checkWorkerPoolSize        uint32
	checkIntervalMs            uint32
	isPaused                   atomic.Bool
	isExited                   atomic.Bool
	grpcDialOpts               []grpc.DialOption
	hardDeleteNodeOverFailTime time.Duration
}

func (h *Checker) ResetCheckWorkerPoolSize(size uint32) {
	h.checkWorkerPoolSize = size
}

func (h *Checker) ResetCheckIntervalMs(ms uint32) {
	h.checkIntervalMs = ms
}

func (h *Checker) Exit() {
	h.isExited.Store(true)
}

func (h *Checker) Pause() {
	h.isPaused.Store(true)
}

func (h *Checker) Resume() {
	h.isPaused.Store(false)
}

func (h *Checker) Run() {
	nodeCh := make(chan *Node)
	exitCh := make(chan struct{})
	go func() {
		var oldWorkerPoolSize uint32
		for {
			if h.isExited.Load() {
				return
			}

			workerPoolSize := h.checkWorkerPoolSize
			if workerPoolSize == 0 {
				workerPoolSize = checkWorkerPoolSize
			}

			if workerPoolSize == oldWorkerPoolSize {
				time.Sleep(time.Duration(h.checkIntervalMs) * time.Millisecond)
				continue
			}

			expandWorkerNum := int32(workerPoolSize) - int32(oldWorkerPoolSize)
			if expandWorkerNum > 0 {
				for i := int32(0); i < expandWorkerNum; i++ {
					h.work(nodeCh, exitCh)
				}
			}

			if expandWorkerNum < 0 {
				for i := expandWorkerNum; i < 0; i++ {
					exitCh <- struct{}{}
				}
			}

			oldWorkerPoolSize = workerPoolSize

			time.Sleep(time.Millisecond * time.Duration(h.checkIntervalMs))
		}
	}()

	for {
		log.Debug("start new cycle")
		if h.isExited.Load() {
			log.Info("checker is exited")
			return
		}

		log.Debug("is checker paused")
		if h.isPaused.Load() {
			sleepMs := 10000
			if h.checkIntervalMs < 1000 {
				sleepMs = int(h.checkIntervalMs)
			}
			log.Info("checker is paused")
			time.Sleep(time.Millisecond * time.Duration(sleepMs))
			continue
		}

		log.Debug("discovery.LoadAll")
		services, err := h.Discovery.LoadAll(context.Background())
		if err != nil {
			log.Errorf("fail to load all services: %v", err)
			time.Sleep(time.Second * 3)
			continue
		}

		start := time.Now()
		log.Debugf("start check services: %+v", services)

		for _, srv := range services {
			for _, node := range srv.Nodes {
				log.Debugf("send service to check chan: %s:%+v", srv.SrvName, node)
				nodeCh <- &Node{
					srvName: srv.SrvName,
					detail:  node,
				}
				log.Debugf("finish send service to check chan: %s:%+v", srv.SrvName, node)
			}
		}

		log.Debugf("finish check services: %+v, cost:%s", services, time.Since(start))

		time.Sleep(time.Millisecond * time.Duration(h.checkIntervalMs))
	}
}

func (h *Checker) work(nodeCh chan *Node, exitCh chan struct{}) {
	go func() {
		for {
			select {
			case <-exitCh:
				log.Important("exit check worker")
				return
			case node := <-nodeCh:
				if err := h.check(node); err != nil {
					log.Error(runtimeutil.NewStackErr(err))
				}
			}
		}
	}()
}

func (h *Checker) check(node *Node) error {
	doCheck := func() (bool, error) {
		addr := fmt.Sprintf("%s:%d", node.detail.Host, node.detail.Port)
		conn, err := grpc.NewClient(addr, h.grpcDialOpts...)
		if err != nil {
			return false, err
		}

		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		ctx = metadata.AppendToOutgoingContext(ctx, easymicrogrpc.CtxKeyRPCAddr, addr)

		log.Debugf("start checker service %s:%+v", node.srvName, node.detail)
		resp, err := healthreporter.NewHealthReporterClient(conn).Ping(ctx, &healthreporter.PingReq{
			PingService: node.srvName,
		})
		if err != nil {
			log.Infof("grpc health check:%s fail %v", node.srvName, err)
			return false, err
		}

		return resp.Ok, nil
	}

	var alive bool
	for retry := 0; retry < 3; retry++ {
		if ok, err := doCheck(); err != nil {
			time.Sleep(5 * time.Second)
			continue
		} else {
			alive = ok
			break
		}
	}

	if alive {
		if !node.detail.Available() {
			removeNodeFailSince(node)
			err := h.Discovery.Register(context.Background(), node.srvName, node.detail)
			if err != nil {
				return err
			}
		}
		return nil
	}

	if h.hardDeleteNodeOverFailTime == 0 {
		err := h.Discovery.Unregister(context.Background(), node.srvName, node.detail, false)
		if err != nil {
			return err
		}
		return nil
	}

	if h.hardDeleteNodeOverFailTime < 0 {
		err := h.Discovery.Unregister(context.Background(), node.srvName, node.detail, true)
		if err != nil {
			return err
		}
		return nil
	}

	since, ok := getNodeFailSince(node)
	if ok {
		if time.Since(*since) > h.hardDeleteNodeOverFailTime {
			err := h.Discovery.Unregister(context.Background(), node.srvName, node.detail, true)
			if err != nil {
				return err
			}
			return nil
		}
	} else {
		saveNodeFailSince(node)
	}

	err := h.Discovery.Unregister(context.Background(), node.srvName, node.detail, false)
	if err != nil {
		return err
	}

	return nil
}
