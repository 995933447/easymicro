package event

import (
	"github.com/995933447/easymicro/example/echo"
	"github.com/995933447/easymicro/node"
	"github.com/995933447/natsevent"
)

func RegisterEventListeners() error {
	err := natsevent.Subscribe(echo.EventNameEcho, node.GetName(), OnEcho)
	if err != nil {
		return err
	}
	return nil
}
