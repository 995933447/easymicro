package node

import (
	"errors"
	"sync"

	"github.com/995933447/gonetutil"
)

var (
	ErrNodeNameAlreadyExists = errors.New("node name already exists")
	ErrNodeHostAlreadyExists = errors.New("node ip already exists")
	ErrNodePortAlreadyExists = errors.New("node port already exists")
)

var (
	name   string
	host   string
	port   int
	nameMu sync.Mutex
	hostMu sync.Mutex
	portMu sync.Mutex
)

func SetName(n string) error {
	if name != "" {
		return ErrNodeNameAlreadyExists
	}

	nameMu.Lock()
	defer nameMu.Unlock()

	name = n
	return nil
}

func GetName() string {
	return name
}

func SetHost(hostVar string) error {
	if host != "" {
		return ErrNodeHostAlreadyExists
	}

	hostMu.Lock()
	defer hostMu.Unlock()
	if host != "" {
		return ErrNodeHostAlreadyExists
	}

	var err error
	host, err = gonetutil.EvalVarToParseIp(hostVar)
	if err != nil {
		return err
	}

	return nil
}

func GetOrAutoSetHost() (string, error) {
	if GetHost() == "" {
		if err := SetHost(gonetutil.InnerIp); err != nil {
			return "", err
		}
	}
	return GetHost(), nil
}

func GetHost() string {
	return host
}

func SetPort(p int) error {
	if port != 0 {
		return ErrNodePortAlreadyExists
	}

	portMu.Lock()
	defer portMu.Unlock()

	port = p
	return nil
}

func GetPort() int {
	return port
}

func GetOrAutoSetPort() (int, error) {
	if p := GetPort(); p != 0 {
		return p, nil
	}

	p := 21000
	for {
		ok, err := gonetutil.IsPortAvailable(p)
		if err != nil {
			return 0, err
		}

		if ok {
			break
		}

		p++
	}

	if err := SetPort(p); err != nil {
		return 0, err
	}

	return GetPort(), nil
}
