package sysmon

import (
	"errors"
	"os"
	"os/signal"
	"sync"
)

var ErrOsSigAliasNotFound = errors.New("os signal alias not found")

func NewOsSignal() *OsSignal {
	return &OsSignal{
		mapSigToCallbackList: make(map[os.Signal][]func()),
	}
}

type OsSignal struct {
	mu                   sync.Mutex
	mapSigToCallbackList map[os.Signal][]func()
	sigAlias             sync.Map
}

func (s *OsSignal) AliasSignal(sig os.Signal, alias string) {
	s.sigAlias.Store(alias, sig)
}

func (s *OsSignal) AppendSignalCallback(sig os.Signal, callback func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	callbackList, _ := s.mapSigToCallbackList[sig]
	s.mapSigToCallbackList[sig] = append(callbackList, callback)
}

func (s *OsSignal) AppendSignalCallbackByAlias(alias string, callback func()) error {
	sigAny, ok := s.sigAlias.Load(alias)
	if !ok {
		return ErrOsSigAliasNotFound
	}

	s.AppendSignalCallback(sigAny.(os.Signal), callback)
	return nil
}

func (s *OsSignal) Start() {
	s.mu.Lock()
	var signals []os.Signal
	for sig := range s.mapSigToCallbackList {
		signals = append(signals, sig)
	}
	s.mu.Unlock()

	ch := make(chan os.Signal, len(signals))
	signal.Notify(ch, signals...)
	go func() {
		defer signal.Stop(ch)
		for sig := range ch {
			s.mu.Lock()
			callbacks := s.mapSigToCallbackList[sig]
			s.mu.Unlock()

			for _, callback := range callbacks {
				callback()
			}
		}
	}()
}
