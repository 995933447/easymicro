package sysmon

import (
	"runtime"

	"github.com/995933447/easymicro/log"
)

func PrintMemStats() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	log.Important("PrintMemStats ===================Begin================")
	log.Importantf("mem.Alloc: %d, %d MB", mem.Alloc, mem.Alloc/1024/1024)
	log.Importantf("mem.TotalAlloc: %d, %d MB", mem.TotalAlloc, mem.TotalAlloc/1024/1024)
	log.Importantf("mem.HeapAlloc: %d, %d MB", mem.HeapAlloc, mem.HeapAlloc/1024/1024)
	log.Importantf("mem.HeapSys: %d, %d MB", mem.HeapSys, mem.HeapSys/1024/1024)
	log.Importantf("MemStats: %+v", &mem)
	log.Important("PrintMemStats ===================End================")
}
