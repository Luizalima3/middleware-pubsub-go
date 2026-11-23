package fila

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Preserva os casos de ordem e concorrência da fila do núcleo anterior,
// adaptados à fila genérica limitada também por bytes.
func TestCapacidadeOrdemEDescartesPorTopico(t *testing.T) {
	type mensagem struct {
		topico string
		id     int
	}
	f := Nova[mensagem](4, 4)
	descartes := map[string]int{}
	for i := 1; i <= 6; i++ {
		topico := "evento/a"
		if i == 2 {
			topico = "evento/b"
		}
		removidas, ok := f.Adicionar(mensagem{topico, i}, 1, true)
		if !ok {
			t.Fatal("recusou mensagem válida")
		}
		for _, m := range removidas {
			descartes[m.topico]++
			if m.id != i-4 {
				t.Fatalf("removeu %d na inserção %d", m.id, i)
			}
		}
	}
	if n, bytes := f.Tamanho(); n != 4 || bytes != 4 {
		t.Fatal(n, bytes)
	}
	for i := 3; i <= 6; i++ {
		m, ok := f.Retirar()
		if !ok || m.id != i {
			t.Fatalf("esperava %d, obteve %+v, ok=%v", i, m, ok)
		}
	}
	if _, ok := f.Retirar(); ok {
		t.Fatal("fila deveria estar vazia")
	}
	if descartes["evento/a"] != 1 || descartes["evento/b"] != 1 {
		t.Fatal(descartes)
	}
}

func TestInsercoesConcorrentesSemConsumidor(t *testing.T) {
	f := Nova[int](16, 16)
	var descartadas atomic.Int64
	pronto := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					r, _ := f.Adicionar(i, 1, true)
					descartadas.Add(int64(len(r)))
				}
			}()
		}
		wg.Wait()
		close(pronto)
	}()
	select {
	case <-pronto:
	case <-time.After(2 * time.Second):
		t.Fatal("inserção bloqueou sem consumidor")
	}
	if n, bytes := f.Tamanho(); n != 16 || bytes != 16 {
		t.Fatal(n, bytes)
	}
	if descartadas.Load() != 8*500-16 {
		t.Fatal(descartadas.Load())
	}
}
