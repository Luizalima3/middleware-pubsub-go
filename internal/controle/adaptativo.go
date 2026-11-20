package controle

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// Adaptativo começa no teto e só reduz por pressão ou ausência de feedback.
type Adaptativo struct {
	mu            sync.Mutex
	maxima, atual float64
	boas          int
	bucket        *Bucket
}

func NovoAdaptativo(teto float64) (*Adaptativo, error) {
	if teto <= 0 || math.IsNaN(teto) || math.IsInf(teto, 0) {
		return nil, errors.New("teto inválido")
	}
	b, e := Novo(teto, 1)
	return &Adaptativo{maxima: teto, atual: teto, bucket: b}, e
}
func (a *Adaptativo) Avaliar(pressao bool, tetoRemoto float64) float64 {
	return a.AvaliarComDemanda(pressao, tetoRemoto, true)
}

func (a *Adaptativo) AvaliarComDemanda(pressao bool, tetoRemoto float64, demanda bool) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	teto := a.maxima
	if tetoRemoto > 0 && !math.IsNaN(tetoRemoto) && !math.IsInf(tetoRemoto, 0) {
		teto = math.Min(teto, tetoRemoto)
	}
	a.atual = math.Min(a.atual, teto)
	if pressao {
		a.atual = math.Max(math.Min(1, teto), a.atual/2)
		a.boas = 0
	} else if demanda {
		a.boas++
		if a.boas >= 2 {
			a.atual = math.Min(teto, a.atual*1.1)
			a.boas = 0
		}
	} else {
		a.boas = 0
	}
	_ = a.bucket.DefinirTaxa(a.atual)
	return a.atual
}
func (a *Adaptativo) Taxa() float64 { a.mu.Lock(); defer a.mu.Unlock(); return a.atual }
func (a *Adaptativo) Aguardar(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if a.bucket.Permitir() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
