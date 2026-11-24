// Package metricas registra recursos do próprio processo e fotografias de filas.
package metricas

// Processo usa CPU acumulada de usuário + kernel e RAM residente atual.
// RAM não é heap Go, memória virtual, pico histórico ou memória de toda a máquina.
type Processo struct {
	CPUSegundos float64
	RAMBytes    uint64
}

func LerProcesso() (Processo, error) { return lerProcesso() }

func percentualCPU(anterior, atual Processo, segundos float64) float64 {
	if segundos <= 0 || atual.CPUSegundos < anterior.CPUSegundos {
		return 0
	}
	return (atual.CPUSegundos - anterior.CPUSegundos) / segundos * 100
}
