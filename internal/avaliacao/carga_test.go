package avaliacao

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"github.com/Luizalima3/middleware-pubsub-go/internal/broker"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCargaUDPComBrokerRealEExportacao(t *testing.T) {
	b, e := broker.Iniciar(broker.Config{Endereco: "127.0.0.1:0", TaxaCliente: 1000, Rajada: 16})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Fechar()
	cfg := Config{URLs: []string{b.Endereco()}, Topico: "teste/avaliacao", Publicadores: 2, Assinantes: 3, Taxa: 10, Duracao: 300 * time.Millisecond, Drenagem: 300 * time.Millisecond, Timeout: 3 * time.Second, Payload: 128, Fila: 8}
	r, e := Executar(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	if r.Resumo.Enviadas != 6 || r.Resumo.Recebidas != 18 || r.Resumo.Ausentes != 0 || r.Resumo.ErrosEnvio != 0 {
		t.Fatalf("%+v", r.Resumo)
	}
	if r.Resumo.P99MS <= 0 || r.Resumo.P99MS < r.Resumo.P50MS {
		t.Fatal("percentis")
	}
	dir := t.TempDir()
	if e = Salvar(dir, r); e != nil {
		t.Fatal(e)
	}
	for _, n := range []string{"resumo.json", "intervalos.csv", "relatorio.html", "latencia.svg", "vazao.svg", "taxa.svg"} {
		d, e := os.ReadFile(filepath.Join(dir, n))
		if e != nil {
			t.Fatal(e)
		}
		if strings.Contains(string(d), "%!") {
			t.Fatal("formatação", n)
		}
		if n == "resumo.json" {
			var s Resumo
			if json.Unmarshal(d, &s) != nil || s.Recebidas != 18 {
				t.Fatal("JSON")
			}
		}
		if strings.HasSuffix(n, ".svg") {
			dec := xml.NewDecoder(strings.NewReader(string(d)))
			for {
				_, e = dec.Token()
				if e == io.EOF {
					break
				}
				if e != nil {
					t.Fatal(e)
				}
			}
		}
	}
}
func TestConfiguracaoInvalida(t *testing.T) {
	c := Config{URLs: []string{"127.0.0.1:1"}, Topico: "x", Publicadores: 1, Assinantes: 1, Taxa: 1, Duracao: time.Second, Timeout: time.Second, Payload: 31, Fila: 1}
	if _, e := c.validar(); e == nil {
		t.Fatal("payload pequeno")
	}
	c.Payload = 128
	c.Taxa = 1e9
	if _, e := c.validar(); e == nil {
		t.Fatal("carga sem limites")
	}
}
func TestPercentis(t *testing.T) {
	var h histograma
	for i := 1; i <= 100; i++ {
		h.registrar(time.Duration(i) * time.Millisecond)
	}
	if x := h.percentil(.99); x < 99 || x > 101.1 {
		t.Fatal(x)
	}
}

func TestManutencaoRapidaNaoReduzConsumidorNormal(t *testing.T) {
	b, e := broker.Iniciar(broker.Config{Endereco: "127.0.0.1:0", TaxaCliente: 500, Rajada: 16, Intervalo: 50 * time.Millisecond, Inatividade: 4 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Fechar()
	cfg := Config{URLs: []string{b.Endereco()}, Topico: "teste/feedback", Publicadores: 1, Assinantes: 2, Taxa: 30, Duracao: 2200 * time.Millisecond, Drenagem: 400 * time.Millisecond, Timeout: 3 * time.Second, Payload: 128, Fila: 8}
	r, e := Executar(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	if r.Resumo.Ausentes != 0 || r.Resumo.Recebidas != r.Resumo.Enviadas*2 {
		t.Fatalf("redução indevida: %+v", r.Resumo)
	}
}
