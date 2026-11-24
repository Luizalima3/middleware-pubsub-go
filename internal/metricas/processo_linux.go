//go:build linux

package metricas

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func lerProcesso() (Processo, error) {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		return Processo{}, err
	}
	d, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return Processo{}, err
	}
	campos := strings.Fields(string(d))
	if len(campos) < 2 {
		return Processo{}, fmt.Errorf("statm inválido")
	}
	pages, err := strconv.ParseUint(campos[1], 10, 64)
	if err != nil {
		return Processo{}, err
	}
	cpu := float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	return Processo{CPUSegundos: cpu, RAMBytes: pages * uint64(os.Getpagesize())}, nil
}
