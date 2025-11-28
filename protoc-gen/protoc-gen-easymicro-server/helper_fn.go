package main

import (
	"path/filepath"
	"text/template"

	"github.com/995933447/stringhelper-go"
)

func Or(args ...bool) bool {
	for _, arg := range args {
		if arg {
			return true
		}
	}
	return false
}

func And(args ...bool) bool {
	for _, arg := range args {
		if !arg {
			return false
		}
	}
	return true
}

func UpperFirstChar(s string) string {
	return stringhelper.UpperFirstASCII(s)
}

func PathBase(path string) string {
	return filepath.Base(path)
}

func LenNatsGRPC(methods []*NatsGRPCMethod) int {
	return len(methods)
}

var funcMap = template.FuncMap{
	"UpperFirstChar": UpperFirstChar,
	"PathBase":       PathBase,
	"LenNatsGRPC":    LenNatsGRPC,
	"And":            And,
	"Or":             Or,
}
