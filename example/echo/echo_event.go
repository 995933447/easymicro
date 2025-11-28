package echo

import (
	"github.com/995933447/natsevent"
)

const (
	EventNameEcho = "echo"
)

type EchoEvent struct {
	Echo string
}

func (e *EchoEvent) Send() error {
	return natsevent.Publish(EventNameEcho, e)
}
