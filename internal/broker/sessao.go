package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/Luizalima3/middleware-pubsub-go/internal/controle"
	"github.com/Luizalima3/middleware-pubsub-go/internal/protocol"
)

// intervaloAvisoDescartes limita os avisos TipoDescartadas a no máximo um por
// tópico a cada 500 ms em cada sessão.
const intervaloAvisoDescartes = 500 * time.Millisecond

// Sessao representa uma conexão WebSocket. Tem duas goroutines:
//
//   - leitor: decodifica, valida, aplica o token bucket e executa os comandos
//     em ordem (o OK do Assinar sai depois de a inscrição estar efetiva);
//   - escritor: única goroutine que escreve dados no socket. Drena primeiro a
//     fila de controle e depois a fila de dados.
//
// Um cliente lento só bloqueia o próprio escritor; o fan-out e os outros
// clientes seguem normalmente.
type Sessao struct {
	id     uint64
	origem string
	b      *Broker
	conn   *websocket.Conn

	ctx      context.Context
	cancelar context.CancelFunc

	// filaControle leva OK, Erro, SemInscritos e Descartadas já codificados.
	// Nunca é fechada; quando cheia, o aviso novo é descartado e contado.
	filaControle chan []byte
	// dados leva os TipoMensagem (buffer circular drop-oldest).
	dados *filaDados

	// bucket é o ponto de integração do controle de taxa da Parte 3,
	// criado uma única vez por conexão.
	bucket *controle.Bucket

	// mu protege inscricoes. Ordem de locks: Topico.mu → Sessao.mu.
	mu         sync.Mutex
	inscricoes map[string]*Topico

	fechada       atomic.Bool
	escritorFeito chan struct{}
}

func novaSessao(b *Broker, conn *websocket.Conn, bucket *controle.Bucket, origem string) *Sessao {
	ctx, cancelar := context.WithCancel(b.ctx)
	return &Sessao{
		id:            b.proximaSessao.Add(1),
		origem:        origem,
		b:             b,
		conn:          conn,
		ctx:           ctx,
		cancelar:      cancelar,
		filaControle:  make(chan []byte, b.cfg.FilaControle),
		dados:         novaFilaDados(b.cfg.FilaAssinante),
		bucket:        bucket,
		inscricoes:    make(map[string]*Topico),
		escritorFeito: make(chan struct{}),
	}
}

// executar roda a sessão até a conexão terminar e então limpa tudo.
// Bloqueia; é chamado na goroutine do handler HTTP.
func (s *Sessao) executar() {
	go s.escritor()
	s.leitor()

	// O leitor terminou: cliente saiu, escritor falhou ou broker encerrando.
	s.fechada.Store(true)
	s.cancelar()
	s.conn.CloseNow()
	<-s.escritorFeito
	s.limpar()
}

// leitor processa os frames recebidos, em ordem.
func (s *Sessao) leitor() {
	for {
		tipo, dados, err := s.conn.Read(s.ctx)
		if err != nil {
			return
		}
		if tipo != websocket.MessageBinary {
			s.responder(protocol.TipoErro, 0, "", CodigoFrameInvalido)
			continue
		}
		m, err := protocol.Decodificar(dados)
		if err != nil {
			s.responder(protocol.TipoErro, 0, "", CodigoFrameInvalido)
			continue
		}
		s.processar(m)
	}
}

// processar executa um comando do cliente.
func (s *Sessao) processar(m protocol.Mensagem) {
	switch m.Tipo {
	case protocol.TipoPublicar:
		s.publicar(m)
	case protocol.TipoAssinar:
		s.assinar(m)
	case protocol.TipoCancelar:
		s.cancelarInscricao(m)
	case protocol.TipoCriarTopico:
		if m.Topico == "" {
			s.responder(protocol.TipoErro, m.ID, m.Topico, CodigoTopicoInvalido)
			return
		}
		s.b.criarTopico(m.Topico)
		s.responder(protocol.TipoOK, m.ID, m.Topico, "")
	case protocol.TipoApagarTopico:
		s.b.apagarTopico(m.Topico)
		s.responder(protocol.TipoOK, m.ID, m.Topico, "")
	default:
		s.responder(protocol.TipoErro, m.ID, m.Topico, CodigoTipoDesconhecido)
	}
}

// publicar valida e enfileira a publicação no tópico, sem bloquear.
// Publicação aceita não gera ACK, para não somar latência.
func (s *Sessao) publicar(m protocol.Mensagem) {
	c := &s.b.cont
	c.publicacoesRecebidas.Add(1)

	// Ponto de integração do token bucket (Parte 3): a resposta de recusa
	// (TipoErro com RATE_LIMIT) vem pronta de controle.Verificar.
	if aviso := s.bucket.Verificar(m); aviso != nil {
		c.publicacoesRejeitadasRateLimit.Add(1)
		s.enfileirarControle(*aviso)
		return
	}

	t := s.b.topico(m.Topico)
	if t == nil {
		c.publicacoesTopicoInexistente.Add(1)
		s.responder(protocol.TipoErro, m.ID, m.Topico, CodigoTopicoInexistente)
		return
	}
	if len(*t.inscritos.Load()) == 0 {
		c.publicacoesSemInscritos.Add(1)
		s.responder(protocol.TipoSemInscritos, m.ID, m.Topico, "")
		return
	}

	// Codifica uma única vez; o mesmo []byte vai para todos os inscritos e
	// nunca é alterado depois.
	frame, err := protocol.Codificar(protocol.Mensagem{
		Tipo:     protocol.TipoMensagem,
		ID:       m.ID,
		Topico:   m.Topico,
		Conteudo: m.Conteudo,
	})
	if err != nil {
		// Não acontece: o frame tem o mesmo tamanho do que foi recebido.
		s.responder(protocol.TipoErro, m.ID, m.Topico, CodigoFrameInvalido)
		return
	}
	if !t.enfileirar(frame) {
		c.publicacoesDescartadasFilaTopico.Add(1)
		s.responder(protocol.TipoErro, m.ID, m.Topico, CodigoTopicoSobrecarregado)
	}
}

// assinar registra a inscrição e só então enfileira o OK.
func (s *Sessao) assinar(m protocol.Mensagem) {
	t := s.b.topico(m.Topico)
	if t == nil || !t.adicionar(s) {
		s.responder(protocol.TipoErro, m.ID, m.Topico, CodigoTopicoInexistente)
		return
	}
	s.responder(protocol.TipoOK, m.ID, m.Topico, "")
}

// cancelarInscricao remove a inscrição, se houver, e responde OK.
func (s *Sessao) cancelarInscricao(m protocol.Mensagem) {
	s.mu.Lock()
	t := s.inscricoes[m.Topico]
	delete(s.inscricoes, m.Topico)
	s.mu.Unlock()
	if t != nil {
		t.remover(s)
	}
	s.responder(protocol.TipoOK, m.ID, m.Topico, "")
}

// entregar é chamado pelo despachante do tópico. Nunca bloqueia: a fila de
// dados descarta a entrega mais antiga quando cheia.
func (s *Sessao) entregar(t *Topico, frame []byte) {
	if s.fechada.Load() {
		return
	}
	c := &s.b.cont
	removido, descartou := s.dados.Inserir(itemFila{topico: t, frame: frame})
	c.entregasEnfileiradas.Add(1)
	if descartou {
		c.entregasDescartadasConsumidorLento.Add(1)
		if removido.topico != nil {
			removido.topico.descartes.Add(1)
		}
	}
}

// responder codifica uma resposta de controle e a coloca na fila de controle.
func (s *Sessao) responder(tipo protocol.Tipo, id uint32, topico, conteudo string) {
	m := protocol.Mensagem{Tipo: tipo, ID: id, Topico: topico}
	if conteudo != "" {
		m.Conteudo = []byte(conteudo)
	}
	s.enfileirarControle(m)
}

// enfileirarControle nunca bloqueia o leitor: com a fila cheia, descarta o
// aviso novo e conta.
func (s *Sessao) enfileirarControle(m protocol.Mensagem) {
	frame, err := protocol.Codificar(m)
	if err != nil {
		return
	}
	select {
	case s.filaControle <- frame:
	default:
		s.b.cont.avisosControleDescartados.Add(1)
	}
}

// escritor é a única goroutine que escreve dados no socket da sessão.
func (s *Sessao) escritor() {
	defer close(s.escritorFeito)

	var ultimoAviso time.Time
	var temporizador *time.Timer
	defer func() {
		if temporizador != nil {
			temporizador.Stop()
		}
	}()

	for {
		// Avisos de descarte por lentidão, no máximo um por tópico a cada
		// intervaloAvisoDescartes. Entram na fila de controle, que tem prioridade.
		if s.dados.TemDescartes() && time.Since(ultimoAviso) >= intervaloAvisoDescartes {
			s.avisarDescartes()
			ultimoAviso = time.Now()
		}

		// 1º: controle.
		select {
		case frame := <-s.filaControle:
			if !s.escrever(frame) {
				return
			}
			continue
		default:
		}

		// 2º: dados.
		if it, ok := s.dados.Retirar(); ok {
			if !s.escrever(it.frame) {
				return
			}
			s.b.cont.entregasEscritas.Add(1)
			continue
		}

		// Ocioso: espera controle, dados, o prazo de um aviso pendente ou o fim.
		var prazo <-chan time.Time
		if s.dados.TemDescartes() {
			espera := max(intervaloAvisoDescartes-time.Since(ultimoAviso), 0)
			if temporizador == nil {
				temporizador = time.NewTimer(espera)
			} else {
				temporizador.Reset(espera)
			}
			prazo = temporizador.C
		}
		select {
		case frame := <-s.filaControle:
			if !s.escrever(frame) {
				return
			}
		case <-s.dados.Sinal():
		case <-prazo:
		case <-s.ctx.Done():
			return
		}
	}
}

// avisarDescartes enfileira um TipoDescartadas por tópico com descartes
// pendentes. Conteudo = uint32 big-endian com a quantidade desde o último aviso.
func (s *Sessao) avisarDescartes() {
	for nome, n := range s.dados.TomarDescartes() {
		var conteudo [4]byte
		binary.BigEndian.PutUint32(conteudo[:], n)
		s.enfileirarControle(protocol.Mensagem{
			Tipo:     protocol.TipoDescartadas,
			Topico:   nome,
			Conteudo: conteudo[:],
		})
	}
}

// escrever envia um frame com timeout. Se o timeout estourar, o cliente está
// travado ou morto: a sessão é encerrada (a biblioteca cliente reconecta).
// Consumidor apenas lento continua conectado e perde mensagens por drop-oldest.
func (s *Sessao) escrever(frame []byte) bool {
	ctx, cancelar := context.WithTimeout(s.ctx, s.b.cfg.TimeoutEscrita)
	err := s.conn.Write(ctx, websocket.MessageBinary, frame)
	// Verifica se o prazo de escrita expirou antes de chamar cancelar().
	// Nota: quando o ctx de Write expira, coder/websocket fecha o socket
	// internamente, o que faz s.leitor() sair e cancelar s.ctx em paralelo;
	// por isso verificamos s.b.ctx.Err() == nil em vez de s.ctx.Err() == nil.
	estourou := errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded)
	cancelar()
	if err == nil {
		return true
	}
	if estourou && s.b.ctx.Err() == nil {
		s.b.cont.sessoesEncerradasTimeoutEscrita.Add(1)
		s.b.logf("sessão %d (%s): timeout de escrita de %s, encerrando", s.id, s.origem, s.b.cfg.TimeoutEscrita)
	}
	s.cancelar()
	s.conn.CloseNow()
	return false
}

// limpar remove a sessão de todos os tópicos e do broker.
func (s *Sessao) limpar() {
	s.mu.Lock()
	topicos := make([]*Topico, 0, len(s.inscricoes))
	for _, t := range s.inscricoes {
		topicos = append(topicos, t)
	}
	s.inscricoes = make(map[string]*Topico)
	s.mu.Unlock()

	for _, t := range topicos {
		t.remover(s)
	}
	s.b.removerSessao(s)
}

// saturar converte um contador para uint32 sem estourar.
func saturar(n uint64) uint32 {
	if n > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n)
}
