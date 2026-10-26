package controle

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestRajadaReposicaoELimite(t *testing.T) {
	b, _ := Novo(10, 2)
	now := time.Unix(100, 0)
	if !b.PermitirEm(now) || !b.PermitirEm(now) || b.PermitirEm(now) {
		t.Fatal("rajada não respeitada")
	}
	if b.PermitirEm(now.Add(50 * time.Millisecond)) {
		t.Fatal("token fracionário foi aceito")
	}
	if !b.PermitirEm(now.Add(100 * time.Millisecond)) {
		t.Fatal("token não reposto")
	}
	if b.PermitirEm(now) {
		t.Fatal("relógio antigo repôs token")
	}
	later := now.Add(time.Hour)
	if !b.PermitirEm(later) || !b.PermitirEm(later) || b.PermitirEm(later) {
		t.Fatal("acúmulo excedeu a capacidade")
	}
	if a, r := b.Contadores(); a != 5 || r != 4 {
		t.Fatalf("contadores: %d/%d", a, r)
	}
}

func TestConcorrenciaEIsolamento(t *testing.T) {
	b, _ := Novo(1, 10)
	now := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); b.PermitirEm(now) }()
	}
	wg.Wait()
	if a, r := b.Contadores(); a != 10 || r != 90 {
		t.Fatalf("%d aceitas, %d rejeitadas", a, r)
	}
	outro, _ := Novo(1, 10)
	if !outro.PermitirEm(now) {
		t.Fatal("um cliente afetou o outro")
	}
}

func TestConfiguracaoInvalida(t *testing.T) {
	for _, taxa := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		if _, err := Novo(taxa, 1); err == nil {
			t.Fatalf("taxa aceita: %v", taxa)
		}
	}
	if _, err := Novo(1, 0); err == nil {
		t.Fatal("rajada zero aceita")
	}
}
