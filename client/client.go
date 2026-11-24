// Package client oferece publicação e controle sobre UDP, com recuperação automática.
package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/controle"
	"github.com/Luizalima3/middleware-pubsub-go/internal/recente"
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
)

var (
	ErrDesconectado = errors.New("cliente sem broker ativo")
	// ErrResultadoDesconhecido indica que um comando enviado pode ter sido executado,
	// mas a biblioteca não recebeu confirmação.
	ErrResultadoDesconhecido = errors.New("resultado da operação é desconhecido")
)

const validadeCachePedidos = 20 * time.Second
const timeoutEscrita = time.Second

func alteraEstado(tipo protocol.Tipo) bool {
	return tipo == protocol.TipoCriarTopico ||
		tipo == protocol.TipoApagarTopico ||
		tipo == protocol.TipoAssinar ||
		tipo == protocol.TipoCancelar
}

func resultadoDesconhecido(causa error) error {
	if causa == nil {
		return ErrResultadoDesconhecido
	}
	return fmt.Errorf("%w: %w", ErrResultadoDesconhecido, causa)
}

type Config struct {
	Brokers                       []string
	ID                            string
	TaxaMaxima                    float64
	Buffer                        int
	Intervalo, Timeout, Repeticao time.Duration
	Tentativas                    int
	// Apenas para avaliar a proteção do broker contra um cliente que ignora a taxa.
	IgnorarControle bool
}
type pedido struct {
	resposta chan protocol.Mensagem
	endereco string
	tipo     protocol.Tipo
}
type Client struct {
	emEnvio             atomic.Int64
	ultimoEnvio         atomic.Int64
	ultimaEstado        time.Time
	enviadas            atomic.Uint64
	duplicadas          atomic.Uint64
	cfg                 Config
	conn                *net.UDPConn
	enderecos           []*net.UDPAddr
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	uma                 sync.Once
	fechado             atomic.Bool
	proximo             atomic.Uint64
	perdidas, recebidas atomic.Uint64
	mu                  sync.Mutex
	atual               int
	conectado           bool
	ultima              time.Time
	topicos             map[string]struct{}
	pendentes           map[uint64]pedido
	comando             sync.Mutex
	envio               sync.Mutex
	eventos             chan protocol.Mensagem
	dados               chan protocol.Mensagem
	avisos              chan protocol.Mensagem
	taxa                *controle.Adaptativo
	vistos              *recente.Cache[bool]
	notificados         *recente.Cache[bool]
}

func Normalizar(endereco string) (string, error) {
	if strings.Contains(endereco, "://") {
		u, e := url.Parse(endereco)
		if e != nil || u.Scheme != "udp" || u.Host == "" || u.Path != "" {
			return "", errors.New("use host:porta ou udp://host:porta")
		}
		return u.Host, nil
	}
	if _, _, e := net.SplitHostPort(endereco); e != nil {
		return "", e
	}
	return endereco, nil
}
func Conectar(ctx context.Context, urls []string) (*Client, error) {
	return ConectarComConfig(ctx, Config{Brokers: urls})
}
func ConectarComConfig(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Brokers) < 1 || len(cfg.Brokers) > 2 {
		return nil, errors.New("informe um ou dois brokers")
	}
	if cfg.TaxaMaxima == 0 {
		cfg.TaxaMaxima = 100
	}
	if cfg.Buffer == 0 {
		cfg.Buffer = 32
	}
	if cfg.Intervalo == 0 {
		cfg.Intervalo = time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 3 * time.Second
	}
	if cfg.Repeticao == 0 {
		cfg.Repeticao = 250 * time.Millisecond
	}
	if cfg.Tentativas == 0 {
		cfg.Tentativas = 3
	}
	if cfg.Buffer < 1 || cfg.Buffer > 10000 || cfg.Intervalo < 10*time.Millisecond || cfg.Timeout < cfg.Intervalo*2 || cfg.Repeticao < time.Millisecond || cfg.Tentativas < 1 || cfg.Tentativas > 10 {
		return nil, errors.New("configuração inválida")
	}
	janelaPorTentativa := (validadeCachePedidos - time.Nanosecond) / time.Duration(cfg.Tentativas)
	if cfg.Repeticao >= janelaPorTentativa || timeoutEscrita >= janelaPorTentativa-cfg.Repeticao {
		return nil, errors.New("a janela de repetição deve ser menor que a validade do cache de comandos do broker")
	}
	ad, e := controle.NovoAdaptativo(cfg.TaxaMaxima)
	if e != nil {
		return nil, e
	}
	var random [8]byte
	if _, e = rand.Read(random[:]); e != nil {
		return nil, e
	}
	if cfg.ID == "" {
		cfg.ID = fmt.Sprintf("cliente-%x", random)
	}
	if len(cfg.ID) > protocol.MaxCliente {
		return nil, errors.New("ID excede 64 bytes")
	}
	var enderecos []*net.UDPAddr
	for _, u := range cfg.Brokers {
		u, e = Normalizar(strings.TrimSpace(u))
		if e != nil {
			return nil, e
		}
		a, e := net.ResolveUDPAddr("udp", u)
		if e != nil {
			return nil, e
		}
		enderecos = append(enderecos, a)
	}
	conn, e := net.ListenUDP("udp", nil)
	if e != nil {
		return nil, e
	}
	vida, cancel := context.WithCancel(context.Background())
	c := &Client{cfg: cfg, conn: conn, enderecos: enderecos, ctx: vida, cancel: cancel, topicos: map[string]struct{}{}, pendentes: map[uint64]pedido{}, eventos: make(chan protocol.Mensagem), dados: make(chan protocol.Mensagem, cfg.Buffer), avisos: make(chan protocol.Mensagem, 64), taxa: ad, vistos: recente.Nova[bool](8192, 20*time.Second), notificados: recente.Nova[bool](2048, 20*time.Second)}
	c.proximo.Store(binary.BigEndian.Uint64(random[:]) >> 1)
	c.wg.Add(2)
	go c.ler()
	go c.distribuir()
	for i := range enderecos {
		c.mu.Lock()
		c.atual = i
		c.mu.Unlock()
		if c.apresentar(ctx) == nil {
			c.mu.Lock()
			c.conectado = true
			c.mu.Unlock()
			c.wg.Add(1)
			go c.manter()
			return c, nil
		}
		if ctx.Err() != nil {
			c.Fechar()
			return nil, ctx.Err()
		}
	}
	c.Fechar()
	return nil, errors.New("nenhum broker respondeu")
}
func (c *Client) Eventos() <-chan protocol.Mensagem { return c.eventos }
func (c *Client) Taxa() float64                     { return c.taxa.Taxa() }
func (c *Client) BrokerAtual() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.conectado {
		return ""
	}
	return c.enderecos[c.atual].String()
}
func (c *Client) Descartadas() uint64 { return c.perdidas.Load() }
func (c *Client) escrever(ctx context.Context, m protocol.Mensagem, a *net.UDPAddr) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.fechado.Load() {
		return ErrDesconectado
	}
	m.Cliente = c.cfg.ID
	dados, e := protocol.Codificar(m)
	if e != nil {
		return e
	}
	c.envio.Lock()
	defer c.envio.Unlock()
	prazo := time.Now().Add(timeoutEscrita)
	if d, ok := ctx.Deadline(); ok && d.Before(prazo) {
		prazo = d
	}
	if e := c.conn.SetWriteDeadline(prazo); e != nil {
		return e
	}
	_, e = c.conn.WriteToUDP(dados, a)
	return e
}
func (c *Client) requisitar(ctx context.Context, m protocol.Mensagem) (protocol.Mensagem, error) {
	c.mu.Lock()
	// As tentativas ficam no mesmo broker: o cache de IDs não é compartilhado.
	a := c.enderecos[c.atual]
	ch := make(chan protocol.Mensagem, 1)
	c.pendentes[m.ID] = pedido{resposta: ch, endereco: a.String(), tipo: m.Tipo}
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pendentes, m.ID); c.mu.Unlock() }()
	enviado := false
	for tentativa := 0; tentativa < c.cfg.Tentativas; tentativa++ {
		if e := c.escrever(ctx, m, a); e != nil {
			if enviado && alteraEstado(m.Tipo) {
				return protocol.Mensagem{}, resultadoDesconhecido(e)
			}
			return protocol.Mensagem{}, e
		}
		enviado = true
		timer := time.NewTimer(c.cfg.Repeticao)
		select {
		case r := <-ch:
			timer.Stop()
			if r.Tipo == protocol.TipoErro {
				return r, fmt.Errorf("broker: %s", r.Conteudo)
			}
			return r, nil
		case <-ctx.Done():
			timer.Stop()
			if enviado && alteraEstado(m.Tipo) {
				return protocol.Mensagem{}, resultadoDesconhecido(ctx.Err())
			}
			return protocol.Mensagem{}, ctx.Err()
		case <-c.ctx.Done():
			timer.Stop()
			if enviado && alteraEstado(m.Tipo) {
				return protocol.Mensagem{}, resultadoDesconhecido(ErrDesconectado)
			}
			return protocol.Mensagem{}, ErrDesconectado
		case <-timer.C:
		}
	}
	if alteraEstado(m.Tipo) {
		return protocol.Mensagem{}, fmt.Errorf("%w: sem confirmação após %d tentativas", ErrResultadoDesconhecido, c.cfg.Tentativas)
	}
	return protocol.Mensagem{}, errors.New("sem resposta após o limite de tentativas")
}
func (c *Client) atividade() protocol.Mensagem {
	return protocol.Mensagem{Tipo: protocol.TipoAtividade, ID: c.proximo.Add(1), Fila: len(c.dados) * 100 / c.cfg.Buffer, Recebidas: c.recebidas.Load(), Perdidas: c.perdidas.Load(), Enviadas: c.enviadas.Load()}
}
func (c *Client) apresentar(ctx context.Context) error {
	_, e := c.requisitar(ctx, c.atividade())
	return e
}
func (c *Client) ler() {
	defer c.wg.Done()
	buf := make([]byte, protocol.MaxMensagem+1)
	esperaErro := 10 * time.Millisecond
	for {
		n, a, e := c.conn.ReadFromUDP(buf)
		if e != nil {
			if c.ctx.Err() != nil {
				return
			}
			timer := time.NewTimer(esperaErro)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if esperaErro < 500*time.Millisecond {
				esperaErro *= 2
				if esperaErro > 500*time.Millisecond {
					esperaErro = 500 * time.Millisecond
				}
			}
			continue
		}
		esperaErro = 10 * time.Millisecond
		m, e := protocol.Decodificar(buf[:n])
		if e != nil {
			continue
		}
		c.mu.Lock()
		valido := a.String() == c.enderecos[c.atual].String()
		p, pendente := c.pendentes[m.ID]
		if pendente && p.endereco != a.String() {
			pendente = false
		}
		if valido {
			c.ultima = time.Now()
		}
		c.mu.Unlock()
		if !valido {
			continue
		}
		if m.Tipo == protocol.TipoEstado {
			c.mu.Lock()
			c.ultimaEstado = time.Now()
			c.mu.Unlock()
			c.taxa.AvaliarComDemanda(m.Fila >= 50, m.Taxa, c.emEnvio.Load() > 0 || time.Since(time.Unix(0, c.ultimoEnvio.Load())) < 2*c.cfg.Intervalo)
			if pendente && p.tipo == protocol.TipoAtividade {
				select {
				case p.resposta <- m:
				default:
				}
			}
			continue
		}
		if (m.Tipo == protocol.TipoOK || m.Tipo == protocol.TipoErro) &&
			m.Aviso == 0 && pendente && p.tipo != protocol.TipoAtividade {
			select {
			case p.resposta <- m:
			default:
			}
			continue
		}
		if m.Aviso != 0 {
			k := m.Origem + ":" + fmt.Sprint(m.Aviso)
			if _, ok := c.notificados.Buscar(k); ok {
				_ = c.escrever(c.ctx, protocol.Mensagem{Tipo: protocol.TipoRecebido, Aviso: m.Aviso}, a)
				continue
			}
			select {
			case c.avisos <- m:
				c.notificados.Guardar(k, true)
				_ = c.escrever(c.ctx, protocol.Mensagem{Tipo: protocol.TipoRecebido, Aviso: m.Aviso}, a)
			default:
				continue
			}
			if m.Tipo == protocol.TipoErro && string(m.Conteudo) == "TOPICO_APAGADO" {
				c.mu.Lock()
				delete(c.topicos, m.Topico)
				c.mu.Unlock()
			}
			continue
		}
		if m.Tipo != protocol.TipoMensagem {
			continue
		}
		k := m.Cliente + ":" + fmt.Sprint(m.ID)
		if _, ok := c.vistos.Buscar(k); ok {
			c.duplicadas.Add(1)
			continue
		}
		c.vistos.Guardar(k, true)
		c.recebidas.Add(1)
		select {
		case c.dados <- m:
		default:
			select {
			case <-c.dados:
				c.perdidas.Add(1)
			default:
			}
			select {
			case c.dados <- m:
			default:
				c.perdidas.Add(1)
			}
		}
	}
}
func (c *Client) distribuir() {
	defer c.wg.Done()
	defer close(c.eventos)
	for {
		var m protocol.Mensagem
		select {
		case <-c.ctx.Done():
			return
		case m = <-c.avisos:
		default:
			select {
			case <-c.ctx.Done():
				return
			case m = <-c.avisos:
			case m = <-c.dados:
			}
		}
		select {
		case c.eventos <- m:
		case <-c.ctx.Done():
			return
		}
	}
}
func (c *Client) manter() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.Intervalo)
	defer ticker.Stop()
	tentativa := 0
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
		c.mu.Lock()
		viva := c.conectado && time.Since(c.ultima) < c.cfg.Timeout
		a := c.enderecos[c.atual]
		c.mu.Unlock()
		if viva {
			c.mu.Lock()
			semFeedback := time.Since(c.ultimaEstado) > 2*c.cfg.Intervalo
			c.mu.Unlock()
			if semFeedback {
				c.taxa.Avaliar(true, 0)
			}
			_ = c.escrever(c.ctx, c.atividade(), a)
			tentativa = 0
			continue
		}
		c.taxa.Avaliar(true, 0)
		c.mu.Lock()
		c.conectado = false
		inicial := c.atual
		c.mu.Unlock()
		// Um comando em andamento termina no endpoint original antes do failover.
		c.comando.Lock()
		recuperou := false
		for passo := 0; passo < len(c.enderecos); passo++ {
			if c.ctx.Err() != nil {
				break
			}
			i := (inicial + passo + 1) % len(c.enderecos)
			c.mu.Lock()
			c.atual = i
			c.mu.Unlock()
			ctx, stop := context.WithTimeout(c.ctx, c.cfg.Timeout)
			err := c.apresentar(ctx)
			if err == nil {
				c.mu.Lock()
				temas := make([]string, 0, len(c.topicos))
				for t := range c.topicos {
					temas = append(temas, t)
				}
				c.mu.Unlock()
				for _, t := range temas {
					_, err = c.requisitar(ctx, protocol.Mensagem{Tipo: protocol.TipoAssinar, ID: c.proximo.Add(1), Topico: t})
					if err != nil {
						break
					}
				}
			}
			stop()
			if err == nil {
				c.mu.Lock()
				c.conectado = true
				c.mu.Unlock()
				recuperou = true
				break
			}
		}
		c.comando.Unlock()
		if !recuperou {
			d := esperaBackoff(tentativa)
			tentativa++
			timer := time.NewTimer(d)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}
func esperaBackoff(tentativa int) time.Duration {
	if tentativa == 0 {
		return 0
	}
	n := tentativa
	if n > 5 {
		n = 5
	}
	d := 200 * time.Millisecond * time.Duration(1<<uint(n-1))
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return d/2 + time.Duration(binary.BigEndian.Uint64(b[:])%uint64(d/2+1))
}
func (c *Client) administrar(ctx context.Context, tipo protocol.Tipo, t string) (uint64, error) {
	c.comando.Lock()
	defer c.comando.Unlock()
	if !protocol.TopicoValido(t) {
		return 0, errors.New("tópico inválido")
	}
	c.mu.Lock()
	viva := c.conectado
	c.mu.Unlock()
	if !viva {
		return 0, ErrDesconectado
	}
	id := c.proximo.Add(1)
	r, e := c.requisitar(ctx, protocol.Mensagem{Tipo: tipo, ID: id, Topico: t})
	if e != nil {
		if errors.Is(e, ErrResultadoDesconhecido) &&
			(tipo == protocol.TipoCancelar || tipo == protocol.TipoApagarTopico) {
			c.mu.Lock()
			delete(c.topicos, t)
			c.mu.Unlock()
		}
		return id, e
	}
	c.mu.Lock()
	if tipo == protocol.TipoAssinar {
		c.topicos[t] = struct{}{}
	}
	if tipo == protocol.TipoCancelar || tipo == protocol.TipoApagarTopico {
		delete(c.topicos, t)
	}
	c.mu.Unlock()
	select {
	case c.avisos <- r:
	default:
	}
	return id, nil
}
func (c *Client) Publicar(ctx context.Context, t string, d []byte) (uint64, error) {
	c.emEnvio.Add(1)
	defer c.emEnvio.Add(-1)
	c.ultimoEnvio.Store(time.Now().UnixNano())
	if !protocol.TopicoValido(t) {
		return 0, errors.New("tópico inválido")
	}
	c.mu.Lock()
	viva := c.conectado
	a := c.enderecos[c.atual]
	c.mu.Unlock()
	if !viva {
		return 0, ErrDesconectado
	}
	id := c.proximo.Add(1)
	m := protocol.Mensagem{Tipo: protocol.TipoPublicar, ID: id, Cliente: c.cfg.ID, Topico: t, Conteudo: d}
	if _, e := protocol.Codificar(m); e != nil {
		return 0, e
	}
	if !c.cfg.IgnorarControle {
		if e := c.taxa.Aguardar(ctx); e != nil {
			return id, e
		}
	}
	c.mu.Lock()
	a = c.enderecos[c.atual]
	viva = c.conectado
	c.mu.Unlock()
	if !viva {
		return id, ErrDesconectado
	}
	err := c.escrever(ctx, m, a)
	if err == nil {
		c.enviadas.Add(1)
	}
	return id, err
}
func (c *Client) Assinar(ctx context.Context, t string) (uint64, error) {
	return c.administrar(ctx, protocol.TipoAssinar, t)
}
func (c *Client) Cancelar(ctx context.Context, t string) (uint64, error) {
	return c.administrar(ctx, protocol.TipoCancelar, t)
}
func (c *Client) CriarTopico(ctx context.Context, t string) (uint64, error) {
	return c.administrar(ctx, protocol.TipoCriarTopico, t)
}
func (c *Client) ApagarTopico(ctx context.Context, t string) (uint64, error) {
	return c.administrar(ctx, protocol.TipoApagarTopico, t)
}
func (c *Client) Estatisticas(ctx context.Context) (protocol.Mensagem, error) {
	c.comando.Lock()
	defer c.comando.Unlock()
	c.mu.Lock()
	viva := c.conectado
	c.mu.Unlock()
	if !viva {
		return protocol.Mensagem{}, ErrDesconectado
	}
	return c.requisitar(ctx, protocol.Mensagem{Tipo: protocol.TipoEstatisticas, ID: c.proximo.Add(1)})
}
func (c *Client) Fechar() error {
	c.uma.Do(func() {
		c.fechado.Store(true)
		c.mu.Lock()
		c.conectado = false
		c.mu.Unlock()
		c.cancel()
		_ = c.conn.Close()
	})
	c.wg.Wait()
	return nil
}

// Duplicadas conta entregas MENSAGEM descartadas pela deduplicação local.
// Não inclui avisos repetidos nem garante reconhecer duplicatas após o cache expirar.
// O contador é acumulado durante toda a vida desta instância do cliente.
func (c *Client) Duplicadas() uint64 { return c.duplicadas.Load() }

// EstadoFilas é uma amostra das duas filas locais, não uma fotografia atômica.
type EstadoFilas struct{ Dados, LimiteDados, Avisos, LimiteAvisos int }

func (c *Client) Filas() EstadoFilas {
	return EstadoFilas{len(c.dados), cap(c.dados), len(c.avisos), cap(c.avisos)}
}
