// Package broker implementa o núcleo do middleware Publish/Subscribe para o
// Cenário C (Interação ao Vivo): baixa latência, descarte de mensagens antigas
// para consumidores lentos (drop-oldest), filas limitadas por tópico e por
// sessão, e ponto de integração com o token bucket por conexão.
package broker

import (
	"context"
	"log"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/Luizalima3/middleware-pubsub-go/internal/controle"
	"github.com/Luizalima3/middleware-pubsub-go/internal/protocol"
)

// Valores padrão da configuração do broker.
const (
	FilaAssinantePadrao  = 1024
	FilaControlePadrao   = 256
	FilaTopicoPadrao     = 4096
	TaxaPadrao           = 100.0
	RajadaPadrao         = 200
	TimeoutEscritaPadrao = 5 * time.Second
)

// Config reúne os parâmetros do núcleo do broker. Campos <= 0 (ou vazios)
// recebem os valores padrão em Novo.
type Config struct {
	// FilaAssinante é a capacidade do buffer circular de dados por inscrito.
	FilaAssinante int
	// FilaControle é a capacidade da fila de avisos de controle por sessão.
	FilaControle int
	// FilaTopico é a capacidade da fila de publicações de cada tópico.
	FilaTopico int
	// Taxa é a reposição em publicações/s do token bucket de cada sessão.
	Taxa float64
	// Rajada é a capacidade inicial de tokens do bucket de cada sessão.
	Rajada int
	// TimeoutEscrita é o prazo máximo de cada escrita no socket.
	TimeoutEscrita time.Duration
	// Origens são os padrões aceitos no handshake WebSocket (OriginPatterns).
	Origens []string
	// Silencioso desliga os logs informativos do núcleo (útil em testes).
	Silencioso bool
}

func (c Config) comPadroes() Config {
	if c.FilaAssinante <= 0 {
		c.FilaAssinante = FilaAssinantePadrao
	}
	if c.FilaControle <= 0 {
		c.FilaControle = FilaControlePadrao
	}
	if c.FilaTopico <= 0 {
		c.FilaTopico = FilaTopicoPadrao
	}
	if c.Taxa <= 0 || math.IsNaN(c.Taxa) || math.IsInf(c.Taxa, 0) {
		c.Taxa = TaxaPadrao
	}
	if c.Rajada < 1 {
		c.Rajada = RajadaPadrao
	}
	if c.TimeoutEscrita <= 0 {
		c.TimeoutEscrita = TimeoutEscritaPadrao
	}
	if len(c.Origens) == 0 {
		c.Origens = []string{"*"}
	}
	return c
}

// Broker mantém os tópicos e as sessões ativas em memória. Todo o estado fica
// contido na struct (sem variáveis globais) para permitir instanciar vários
// brokers no mesmo processo ou evoluir para coordenação entre instâncias.
type Broker struct {
	cfg Config

	ctx      context.Context
	cancelar context.CancelFunc

	mu      sync.RWMutex
	topicos map[string]*Topico

	muSessoes sync.Mutex
	sessoes   map[*Sessao]struct{}

	proximaSessao atomic.Uint64
	fechado       atomic.Bool
	wg            sync.WaitGroup // despachantes e sessões em andamento

	cont contadores
}

// Novo cria um Broker pronto para uso.
func Novo(cfg Config) *Broker {
	cfg = cfg.comPadroes()
	ctx, cancelar := context.WithCancel(context.Background())
	return &Broker{
		cfg:      cfg,
		ctx:      ctx,
		cancelar: cancelar,
		topicos:  make(map[string]*Topico),
		sessoes:  make(map[*Sessao]struct{}),
	}
}

// Handler devolve o http.Handler do broker com as rotas:
//   - /ws: endpoint WebSocket do protocolo binário;
//   - GET /contadores: fotografia em JSON dos contadores do núcleo.
//
// Por compatibilidade com testes que passam a URL base do httptest.Server sem
// caminho, uma requisição com cabeçalho Upgrade: websocket na raiz "/" também
// é atendida como WebSocket.
func (b *Broker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", b.tratarWS)
	mux.HandleFunc("/contadores", b.tratarContadores)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Header.Get("Upgrade") != "" {
			b.tratarWS(w, r)
			return
		}
		http.NotFound(w, r)
	})
	return mux
}

// tratarWS aceita a conexão WebSocket e conduz a sessão até o fim.
func (b *Broker) tratarWS(w http.ResponseWriter, r *http.Request) {
	if b.fechado.Load() {
		http.Error(w, "broker encerrado", http.StatusServiceUnavailable)
		return
	}
	bucket, err := controle.Novo(b.cfg.Taxa, b.cfg.Rajada)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  b.cfg.Origens,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		b.logf("handshake websocket falhou (%s): %v", r.RemoteAddr, err)
		return
	}
	conn.SetReadLimit(protocol.MaxMensagem)

	s := novaSessao(b, conn, bucket, r.RemoteAddr)
	if !b.registrarSessao(s) {
		_ = conn.Close(websocket.StatusGoingAway, "broker encerrando")
		return
	}
	defer b.wg.Done()
	s.executar()
}

func (b *Broker) registrarSessao(s *Sessao) bool {
	b.muSessoes.Lock()
	defer b.muSessoes.Unlock()
	if b.fechado.Load() {
		return false
	}
	b.sessoes[s] = struct{}{}
	b.cont.conexoesAtivas.Add(1)
	b.wg.Add(1)
	return true
}

func (b *Broker) removerSessao(s *Sessao) {
	b.muSessoes.Lock()
	_, existia := b.sessoes[s]
	delete(b.sessoes, s)
	b.muSessoes.Unlock()
	if existia {
		b.cont.conexoesAtivas.Add(-1)
	}
}

// topico busca um tópico existente.
func (b *Broker) topico(nome string) *Topico {
	b.mu.RLock()
	t := b.topicos[nome]
	b.mu.RUnlock()
	return t
}

// criarTopico garante que o tópico exista (idempotente).
func (b *Broker) criarTopico(nome string) *Topico {
	b.mu.RLock()
	t := b.topicos[nome]
	b.mu.RUnlock()
	if t != nil {
		return t
	}

	b.mu.Lock()
	if t = b.topicos[nome]; t != nil {
		b.mu.Unlock()
		return t
	}
	if b.fechado.Load() {
		b.mu.Unlock()
		return nil
	}
	t = novoTopico(b, nome)
	b.topicos[nome] = t
	b.wg.Add(1)
	b.mu.Unlock()

	go t.despachar()
	return t
}

// apagarTopico remove o tópico, para seu despachante e avisa os inscritos com
// TipoErro (ID 0, conteúdo TOPICO_APAGADO) fora de qualquer lock. Idempotente.
func (b *Broker) apagarTopico(nome string) {
	b.mu.Lock()
	t := b.topicos[nome]
	delete(b.topicos, nome)
	b.mu.Unlock()
	if t == nil {
		return
	}
	inscritos := t.apagar()
	aviso := protocol.Mensagem{
		Tipo:     protocol.TipoErro,
		ID:       0,
		Topico:   nome,
		Conteudo: []byte(CodigoTopicoApagado),
	}
	for _, s := range inscritos {
		s.enfileirarControle(aviso)
	}
}

// Fechar encerra o broker graciosamente: envia StatusGoingAway a todas as
// sessões ativas, para os despachantes dos tópicos e aguarda o término das
// goroutines.
func (b *Broker) Fechar() error {
	if !b.fechado.CompareAndSwap(false, true) {
		b.wg.Wait()
		return nil
	}

	b.muSessoes.Lock()
	sessoes := make([]*Sessao, 0, len(b.sessoes))
	for s := range b.sessoes {
		sessoes = append(sessoes, s)
	}
	b.muSessoes.Unlock()

	// Envia o frame Close com StatusGoingAway com prazo curto antes de
	// cancelar o contexto das sessões.
	var wgClose sync.WaitGroup
	for _, s := range sessoes {
		wgClose.Add(1)
		go func(s *Sessao) {
			defer wgClose.Done()
			_ = s.conn.Close(websocket.StatusGoingAway, "broker encerrando")
		}(s)
	}
	pronto := make(chan struct{})
	go func() {
		wgClose.Wait()
		close(pronto)
	}()
	select {
	case <-pronto:
	case <-time.After(500 * time.Millisecond):
	}

	b.cancelar()
	for _, s := range sessoes {
		s.conn.CloseNow()
	}

	b.mu.Lock()
	for nome, t := range b.topicos {
		t.parar()
		delete(b.topicos, nome)
	}
	b.mu.Unlock()

	b.wg.Wait()
	return nil
}

func (b *Broker) logf(formato string, args ...any) {
	if !b.cfg.Silencioso {
		log.Printf(formato, args...)
	}
}
