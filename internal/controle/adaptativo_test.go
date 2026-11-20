package controle

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestAdaptacaoMantemReduzRecuperaETeto(t *testing.T) {
	a, e := NovoAdaptativo(100)
	if e != nil {
		t.Fatal(e)
	}
	if a.Avaliar(false, 100) != 100 {
		t.Fatal("reduziu sem pressão")
	}
	if a.Avaliar(true, 100) != 50 {
		t.Fatal("não reduziu pela metade")
	}
	for i := 0; i < 5; i++ {
		if a.AvaliarComDemanda(false, 100, false) != 50 {
			t.Fatal("aumentou sem demanda")
		}
	}
	if a.Avaliar(false, 100) != 50 {
		t.Fatal("recuperou antes de duas avaliações")
	}
	if x := a.Avaliar(false, 100); math.Abs(x-55) > .001 {
		t.Fatal(x)
	}
	for i := 0; i < 100; i++ {
		a.Avaliar(false, 1000)
	}
	if a.Taxa() > 100 {
		t.Fatal("ultrapassou teto local")
	}
	if a.Avaliar(false, 10) > 10 {
		t.Fatal("ultrapassou teto remoto")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if a.Aguardar(ctx) == nil {
		t.Fatal("ignorou cancelamento")
	}
}
func TestMudarTaxaNaoRenovaRajada(t *testing.T) {
	b, _ := Novo(.001, 1)
	if !b.Permitir() {
		t.Fatal("primeira ficha")
	}
	if e := b.DefinirTaxa(.001); e != nil {
		t.Fatal(e)
	}
	if b.Permitir() {
		t.Fatal("mudança renovou rajada")
	}
	if b.DefinirTaxa(math.NaN()) == nil {
		t.Fatal("aceitou NaN")
	}
}
func TestEsperaRespeitaPrazo(t *testing.T) {
	a, _ := NovoAdaptativo(.01)
	if a.Aguardar(context.Background()) != nil {
		t.Fatal("primeiro envio")
	}
	ctx, c := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer c()
	if a.Aguardar(ctx) == nil {
		t.Fatal("deveria terminar pelo prazo")
	}
}
