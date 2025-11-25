package log

import "testing"

func TestLog(t *testing.T) {
	Debug("this is a debug log")
	Info("this is a info log")
}
