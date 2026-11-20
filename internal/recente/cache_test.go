package recente

import (
	"testing"
	"time"
)

func TestCapacidadeTTLAtualizacao(t *testing.T) {
	c := Nova[int](2, 20*time.Millisecond)
	c.Guardar("a", 1)
	c.Guardar("a", 2)
	c.Guardar("b", 3)
	if n, ok := c.Buscar("a"); !ok || n != 2 {
		t.Fatal(n, ok)
	}
	c.Guardar("c", 4)
	if _, ok := c.Buscar("a"); ok {
		t.Fatal("não limitou quantidade")
	}
	time.Sleep(25 * time.Millisecond)
	if _, ok := c.Buscar("c"); ok {
		t.Fatal("não expirou")
	}
}
