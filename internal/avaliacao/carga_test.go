package avaliacao

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/controle"
	"github.com/Luizalima3/middleware-pubsub-go/internal/protocol"
	"github.com/coder/websocket"
)

// brokerTeste existe somente nos testes; não é o núcleo de produção da pessoa 1.
func brokerTeste(t *testing.T, limitar, descartar, eco bool) string {
	t.Helper()
	var mu sync.Mutex
	conns := map[*websocket.Conn]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		c.SetReadLimit(protocol.MaxMensagem)
		mu.Lock()
		conns[c] = false
		mu.Unlock()
		defer func() { mu.Lock(); delete(conns, c); mu.Unlock(); c.CloseNow() }()
		bucket, _ := controle.Novo(.0001, 2)
		enviar := func(dest *websocket.Conn, m protocol.Mensagem) {
			d, e := protocol.Codificar(m)
			if e == nil {
				x, cancel := context.WithTimeout(r.Context(), time.Second)
				_ = dest.Write(x, websocket.MessageBinary, d)
				cancel()
			}
		}
		for {
			_, d, e := c.Read(r.Context())
			if e != nil {
				return
			}
			m, e := protocol.Decodificar(d)
			if e != nil {
				continue
			}
			if eco {
				enviar(c, m)
				continue
			}
			switch m.Tipo {
			case protocol.TipoCriarTopico:
				enviar(c, protocol.Mensagem{Tipo: protocol.TipoOK, ID: m.ID})
			case protocol.TipoAssinar:
				mu.Lock()
				conns[c] = true
				mu.Unlock()
				enviar(c, protocol.Mensagem{Tipo: protocol.TipoOK, ID: m.ID})
			case protocol.TipoPublicar:
				if limitar && !bucket.Permitir() {
					enviar(c, protocol.Mensagem{Tipo: protocol.TipoErro, ID: m.ID, Topico: m.Topico, Conteudo: []byte("RATE_LIMIT")})
					continue
				}
				if descartar {
					continue
				}
				mu.Lock()
				var dests []*websocket.Conn
				for dest, sub := range conns {
					if sub {
						dests = append(dests, dest)
					}
				}
				mu.Unlock()
				for _, dest := range dests {
					enviar(dest, protocol.Mensagem{Tipo: protocol.TipoMensagem, ID: m.ID, Topico: m.Topico, Conteudo: m.Conteudo})
				}
			}
		}
	}))
	t.Cleanup(func() {
		mu.Lock()
		for c := range conns {
			c.CloseNow()
		}
		mu.Unlock()
		srv.Close()
	})
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func configuracao(url string) Config {
	return Config{URLs: []string{url}, Topico: "teste/unico", Publicadores: 2, Assinantes: 3, Taxa: 10, Duracao: 200 * time.Millisecond, Drenagem: 150 * time.Millisecond, Timeout: 2 * time.Second, Payload: 64, Fila: 8}
}

func TestCargaEntregaFanoutERelatorio(t *testing.T) {
	r, err := Executar(context.Background(), configuracao(brokerTeste(t, false, false, false)))
	if err != nil {
		t.Fatal(err)
	}
	s := r.Resumo
	if s.Enviadas != 4 || s.Recebidas != 12 || s.Ausentes != 0 || s.ErrosEnvio != 0 || s.Duplicadas != 0 {
		t.Fatalf("contadores inesperados: %+v", s)
	}
	if s.P99MS <= 0 || s.P99MS < s.P50MS {
		t.Fatal("percentis inválidos")
	}
	dir := t.TempDir()
	if err = Salvar(dir, r); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"resumo.json", "intervalos.csv", "relatorio.html", "latencia.svg", "vazao.svg"} {
		d, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(d), "%!") {
			t.Fatalf("formatação quebrada em %s", name)
		}
		if strings.HasSuffix(name, ".svg") {
			dec := xml.NewDecoder(strings.NewReader(string(d)))
			for {
				_, e := dec.Token()
				if e == io.EOF {
					break
				}
				if e != nil {
					t.Fatal(e)
				}
			}
		}
		if name == "resumo.json" {
			var x Resumo
			if err = json.Unmarshal(d, &x); err != nil || x.Recebidas != 12 {
				t.Fatal("resumo JSON inválido", err)
			}
		}
	}
}

func TestAusentesNaoSaoConfundidasComEnvios(t *testing.T) {
	r, err := Executar(context.Background(), configuracao(brokerTeste(t, false, true, false)))
	if err != nil {
		t.Fatal(err)
	}
	if r.Resumo.Enviadas != 4 || r.Resumo.Recebidas != 0 || r.Resumo.Ausentes != 12 {
		t.Fatalf("%+v", r.Resumo)
	}
}

func TestAvisoDeRateLimit(t *testing.T) {
	cfg := configuracao(brokerTeste(t, true, false, false))
	cfg.Publicadores = 1
	cfg.Taxa = 50
	r, err := Executar(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Resumo.Enviadas != 10 || r.Resumo.Rejeitadas != 8 || r.Resumo.Recebidas != 6 || r.Resumo.Ausentes != 24 {
		t.Fatalf("%+v", r.Resumo)
	}
}

func TestEcoNaoEConfundidoComBrokerReal(t *testing.T) {
	cfg := configuracao(brokerTeste(t, false, false, true))
	cfg.Timeout = 100 * time.Millisecond
	_, err := Executar(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "TipoOK") {
		t.Fatalf("erro esperado de preparação: %v", err)
	}
}

func TestLimitesDeCarga(t *testing.T) {
	cfg := configuracao("ws://localhost/ws")
	cfg.Payload = 31
	if _, err := cfg.validar(); err == nil {
		t.Fatal("payload curto aceito")
	}
	cfg = configuracao("ws://localhost/ws")
	cfg.Taxa = 1e9
	if _, err := cfg.validar(); err == nil {
		t.Fatal("carga excessiva aceita")
	}
}

func TestHistogramaPercentis(t *testing.T) {
	var h histograma
	for i := 1; i <= 100; i++ {
		h.registrar(time.Duration(i) * time.Millisecond)
	}
	for _, v := range []struct{ p, ms float64 }{{.5, 50}, {.95, 95}, {.99, 99}} {
		x := h.percentil(v.p)
		if x < v.ms || x > v.ms*1.021 {
			t.Fatalf("p%.0f: %.4f", v.p*100, x)
		}
	}
}
