package event

import (
	"fmt"

	"github.com/995933447/easymicro/example/echo"
)

func OnEcho(evt *echo.EchoEvent) error {
	fmt.Println("OnEcho", evt.Echo)
	return nil
}
