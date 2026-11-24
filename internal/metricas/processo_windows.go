//go:build windows

package metricas

import (
	"fmt"
	"syscall"
	"unsafe"
)

var (
	kernel32             = syscall.NewLazyDLL("kernel32.dll")
	getCurrentProcess    = kernel32.NewProc("GetCurrentProcess")
	getProcessTimes      = kernel32.NewProc("GetProcessTimes")
	getProcessMemoryInfo = syscall.NewLazyDLL("psapi.dll").NewProc("GetProcessMemoryInfo")
)

// PROCESS_MEMORY_COUNTERS_EX: DWORDs seguidos de SIZE_Ts (32 ou 64 bits).
type memoriaProcesso struct {
	CB, FalhasPagina                                                                        uint32
	PicoRAM, RAM, PicoPaged, Paged, PicoNonPaged, NonPaged, Pagefile, PicoPagefile, Private uintptr
}

func lerProcesso() (Processo, error) {
	h, _, _ := getCurrentProcess.Call()
	var criacao, saida, kernel, usuario syscall.Filetime
	ok, _, err := getProcessTimes.Call(h, uintptr(unsafe.Pointer(&criacao)), uintptr(unsafe.Pointer(&saida)), uintptr(unsafe.Pointer(&kernel)), uintptr(unsafe.Pointer(&usuario)))
	if ok == 0 {
		return Processo{}, fmt.Errorf("GetProcessTimes: %w", err)
	}
	var m memoriaProcesso
	m.CB = uint32(unsafe.Sizeof(m))
	ok, _, err = getProcessMemoryInfo.Call(h, uintptr(unsafe.Pointer(&m)), uintptr(m.CB))
	if ok == 0 {
		return Processo{}, fmt.Errorf("GetProcessMemoryInfo: %w", err)
	}
	ticks := uint64(kernel.HighDateTime)<<32 | uint64(kernel.LowDateTime)
	ticks += uint64(usuario.HighDateTime)<<32 | uint64(usuario.LowDateTime)
	return Processo{CPUSegundos: float64(ticks) / 1e7, RAMBytes: uint64(m.RAM)}, nil
}
