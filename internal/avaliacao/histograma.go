package avaliacao

import (
	"math"
	"time"
)

// histograma tem memória fixa. Os limites superiores têm resolução de 2%.
// É utilizado sob o mutex do coletor; não é seguro para uso concorrente isolado.
type histograma struct {
	bins [2048]uint64
	n    uint64
}

func (h *histograma) registrar(d time.Duration) {
	i := 0
	if d > 1 {
		i = int(math.Log(float64(d)) / math.Log(1.02))
	}
	if i >= len(h.bins) {
		i = len(h.bins) - 1
	}
	h.bins[i]++
	h.n++
}

func (h *histograma) percentil(p float64) float64 {
	if h.n == 0 {
		return 0
	}
	alvo := uint64(math.Ceil(float64(h.n) * p))
	var soma uint64
	for i, n := range h.bins {
		soma += n
		if soma >= alvo {
			return math.Ceil(math.Pow(1.02, float64(i+1))) / 1e6
		}
	}
	return 0
}
