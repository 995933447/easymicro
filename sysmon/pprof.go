package sysmon

import (
	"os"
	"runtime/pprof"
	"time"
)

func DumpCPUProfile(outputFile string, sampleTimeLong time.Duration) error {
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}

	defer f.Close()

	if err = pprof.StartCPUProfile(f); err != nil {
		return err
	}

	defer pprof.StopCPUProfile()

	time.Sleep(sampleTimeLong)

	return nil
}

func DumpHeapProfile(outputFile string) error {
	f, err := os.Create(outputFile)
	if err != nil {
		return err
	}

	defer f.Close()

	if err = pprof.WriteHeapProfile(f); err != nil {
		return err
	}

	return nil
}

func DumpPProfiles(outputCPUFile, outputHeapFile string, sampleTimeLong time.Duration) error {
	if err := DumpCPUProfile(outputCPUFile, sampleTimeLong); err != nil {
		return err
	}

	if err := DumpHeapProfile(outputHeapFile); err != nil {
		return err
	}

	return nil
}
