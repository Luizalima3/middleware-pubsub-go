package client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/Luizalima3/middleware-pubsub-go/internal/protocol"
)

// ErrDesconectado é devolvido quando não há conexão ativa com o broker.
var ErrDesconectado = errors.New("cliente desconectado do broker")

const (
	baseEspera = 200 * time.Millisecond
	maxEspera  = 5 * time.Second
)

// Client mantém a conexão com o broker e reconecta sozinho se ela cair.
type Client struct {
	urls       []string
	currentIdx int
	proximoID  atomic.Uint32
	eventos    chan protocol.Mensagem

	ctx      context.Context // vive até o Fechar
	cancelar context.CancelFunc
	fechado  atomic.Bool
	feito    chan struct{}

	mu      sync.Mutex
	conn    *websocket.Conn     // nil enquanto desconectado
	topicos map[string]struct{} // tópicos assinados, para reassinar
}

// abrir faz o handshake WebSocket com o broker.
func abrir(ctx context.Context, url string) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, fmt.Errorf("conectar ao broker %s: %w", url, err)
	}
	conn.SetReadLimit(protocol.MaxMensagem)
	return conn, nil
}

// Conectar abre a primeira conexão tentando os brokers da lista.
func Conectar(ctx context.Context, urls []string) (*Client, error) {
	if len(urls) == 0 {
		return nil, errors.New("lista de brokers vazia")
	}

	var conn *websocket.Conn
	var err error
	
	// Tenta conectar no primeiro broker disponível da lista
	for i, url := range urls {
		conn, err = abrir(ctx, url)
		if err == nil {
			break
		}
		log.Printf("falha ao conectar no broker %s: %v", url, err)
		if i == len(urls)-1 {
			return nil, fmt.Errorf("nenhum broker disponível: %w", err)
		}
	}

	ctxVida, cancelar := context.WithCancel(context.Background())
	c := &Client{
		urls:      urls,
		eventos:  make(chan protocol.Mensagem, 256),
		ctx:      ctxVida,
		cancelar: cancelar,
		feito:    make(chan struct{}),
		conn:     conn,
		topicos:  make(map[string]struct{}),
	}
	go c.manter(conn)
	return c, nil
}

// manter lê da conexão atual e, quando ela cai, reconecta com backoff.
func (c *Client) manter(conn *websocket.Conn) {
	defer close(c.feito)
	defer close(c.eventos)

	tentativa := 0
	for {
		c.ler(conn)

		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
		conn.CloseNow()

		for { 
			if c.fechado.Load() || c.ctx.Err() != nil {
				return
			}
			select {
			case <-time.After(esperaBackoff(tentativa)):
			case <-c.ctx.Done():
				return
			}
			tentativa++

			// Failover: tenta o próximo broker da lista
			c.mu.Lock()
			url := c.urls[c.currentIdx]
			c.currentIdx = (c.currentIdx + 1) % len(c.urls)
			c.mu.Unlock()

			ctxDial, cancelarDial := context.WithTimeout(c.ctx, 5*time.Second)
			novo, err := abrir(ctxDial, url)
			cancelarDial()
			if err != nil {
				log.Printf("reconexão falhou no broker %s (tentativa %d): %v", url, tentativa, err)
				continue
			}
			if err := c.reassinar(novo); err != nil {
				log.Printf("reassinar falhou: %v", err)
				novo.CloseNow()
				continue
			}
			log.Printf("reconectado ao broker %s", url)
			conn = novo
			tentativa = 0
			break
		}
	}
}

// ler decodifica os frames da conexão e os entrega em c.eventos.
func (c *Client) ler(conn *websocket.Conn) {
	for {
		_, dados, err := conn.Read(c.ctx)
		if err != nil {
			return
		}
		m, err := protocol.Decodificar(dados)
		if err != nil {
			continue // frame inválido: ignora por enquanto
		}
		select {
		case c.eventos <- m:
		case <-c.ctx.Done():
			return
		}
	}
}

// esperaBackoff: a 1ª tentativa é imediata; as seguintes dobram a espera
// até um teto, com jitter (sorteio) para os clientes não reconectarem juntos.
func esperaBackoff(tentativa int) time.Duration {
	if tentativa == 0 {
		return 0
	}
	d := maxEspera
	if tentativa-1 < 6 {
		d = baseEspera << (tentativa - 1)
		if d > maxEspera {
			d = maxEspera
		}
	}
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

// reassinar reenvia os Assinar na conexão nova e só então a torna ativa.
func (c *Client) reassinar(conn *websocket.Conn) error {
	c.mu.Lock()
	topicos := make([]string, 0, len(c.topicos))
	for t := range c.topicos {
		topicos = append(topicos, t)
	}
	c.mu.Unlock()

	ctx, cancelar := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancelar()
	for _, t := range topicos {
		dados, err := protocol.Codificar(protocol.Mensagem{
			Tipo:   protocol.TipoAssinar,
			ID:     c.proximoID.Add(1),
			Topico: t,
		})
		if err != nil {
			return err
		}
		if err := conn.Write(ctx, websocket.MessageBinary, dados); err != nil {
			return err
		}
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

// Eventos devolve o canal por onde chegam as mensagens do broker.
// Só é fechado quando o cliente é fechado com Fechar.
func (c *Client) Eventos() <-chan protocol.Mensagem {
	return c.eventos
}

// enviar monta e envia um frame. Devolve o ID usado.
func (c *Client) enviar(ctx context.Context, tipo protocol.Tipo, topico string, conteudo []byte) (uint32, error) {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return 0, ErrDesconectado
	}

	id := c.proximoID.Add(1)
	dados, err := protocol.Codificar(protocol.Mensagem{
		Tipo:     tipo,
		ID:       id,
		Topico:   topico,
		Conteudo: conteudo,
	})
	if err != nil {
		return 0, fmt.Errorf("codificar mensagem: %w", err)
	}
	if err := conn.Write(ctx, websocket.MessageBinary, dados); err != nil {
		return 0, fmt.Errorf("enviar mensagem: %w", err)
	}
	return id, nil
}

// Publicar envia conteúdo para um tópico. Se estiver desconectado,
// devolve ErrDesconectado (a mensagem não é guardada).
func (c *Client) Publicar(ctx context.Context, topico string, conteudo []byte) (uint32, error) {
	return c.enviar(ctx, protocol.TipoPublicar, topico, conteudo)
}

// Assinar inscreve o cliente em um tópico e o lembra para reassinar.
func (c *Client) Assinar(ctx context.Context, topico string) (uint32, error) {
	id, err := c.enviar(ctx, protocol.TipoAssinar, topico, nil)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.topicos[topico] = struct{}{}
	c.mu.Unlock()
	return id, nil
}

// Cancelar remove a inscrição e deixa de reassinar o tópico.
func (c *Client) Cancelar(ctx context.Context, topico string) (uint32, error) {
	c.mu.Lock()
	delete(c.topicos, topico)
	c.mu.Unlock()
	return c.enviar(ctx, protocol.TipoCancelar, topico, nil)
}

// CriarTopico solicita a criação de um tópico no broker.
func (c *Client) CriarTopico(ctx context.Context, topico string) (uint32, error) {
	return c.enviar(ctx, protocol.TipoCriarTopico, topico, nil)
}

// ApagarTopico solicita a exclusão de um tópico no broker.
func (c *Client) ApagarTopico(ctx context.Context, topico string) (uint32, error) {
	return c.enviar(ctx, protocol.TipoApagarTopico, topico, nil)
}

// Fechar encerra a conexão e para a reconexão automática.
func (c *Client) Fechar() error {
	c.fechado.Store(true) // antes de tudo, para não tentar reconectar
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	var err error
	if conn != nil {
		err = conn.Close(websocket.StatusNormalClosure, "cliente encerrando")
	}
	c.cancelar()
	<-c.feito
	return err
}
