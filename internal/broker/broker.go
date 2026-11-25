// Package broker distribui mensagens UDP/JSON entre clientes e um único par.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Luizalima3/middleware-pubsub-go/internal/controle"
	"github.com/Luizalima3/middleware-pubsub-go/internal/fila"
	"github.com/Luizalima3/middleware-pubsub-go/internal/recente"
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Endereco, Par, ID                                          string
	TaxaCliente, TaxaEntrada, TaxaSaida, TaxaControle, TaxaPar float64
	Rajada, FilaTopico, FilaCliente, BytesTopico, BytesCliente int
	MaxClientes, MaxTopicos, MaxPendencias                     int
	CacheRespostas, CacheVistos, CacheAvisos                   int
	Intervalo, Inatividade, Repeticao, ValidadeCache           time.Duration
	Tentativas                                                 int
}

func (c Config) padroes() (Config, error) {
	if c.Endereco == "" {
		c.Endereco = "127.0.0.1:8080"
	}
	rates := []*float64{&c.TaxaCliente, &c.TaxaEntrada, &c.TaxaSaida, &c.TaxaControle, &c.TaxaPar}
	defs := []float64{100, 2000, 5000, 200, 1000}
	for i, p := range rates {
		if *p == 0 {
			*p = defs[i]
		}
		if *p < 0 || math.IsNaN(*p) || math.IsInf(*p, 0) {
			return c, errors.New("taxas devem ser finitas e positivas")
		}
	}
	ints := []*int{&c.Rajada, &c.FilaTopico, &c.FilaCliente, &c.BytesTopico, &c.BytesCliente, &c.MaxClientes, &c.MaxTopicos, &c.MaxPendencias, &c.Tentativas, &c.CacheRespostas, &c.CacheVistos, &c.CacheAvisos}
	vals := []int{4, 128, 64, 128000, 64000, 256, 128, 512, 3, 2048, 8192, 2048}
	for i, p := range ints {
		if *p == 0 {
			*p = vals[i]
		}
		if *p < 1 {
			return c, errors.New("limites devem ser positivos")
		}
	}
	if c.Intervalo == 0 {
		c.Intervalo = time.Second
	}
	if c.Inatividade == 0 {
		c.Inatividade = 5 * time.Second
	}
	if c.Repeticao == 0 {
		c.Repeticao = 300 * time.Millisecond
	}
	if c.ValidadeCache == 0 {
		c.ValidadeCache = 20 * time.Second
	}
	if c.Intervalo < 10*time.Millisecond || c.Inatividade < c.Intervalo*3 || c.Repeticao < time.Millisecond || c.ValidadeCache < time.Second {
		return c, errors.New("intervalos inválidos")
	}
	if len(c.ID) > protocol.MaxCliente {
		return c, errors.New("ID excede o limite")
	}
	return c, nil
}

type entrega struct {
	m     protocol.Mensagem
	dados []byte
}
type topico struct {
	nome      string
	fila      *fila.Fila[entrega]
	inscritos map[string]*sessao
	ctx       context.Context
	cancel    context.CancelFunc
}
type sessao struct {
	emEnvio                                                                   bool
	feedbackEm                                                                time.Time
	envios, recebidasBase, gapAnterior, publicacoes, enviadasBase, gapEntrada uint64
	feedbackInicial                                                           bool
	perdasSeguidas, perdasEntrada                                             int
	id                                                                        string
	addr                                                                      *net.UDPAddr
	visto                                                                     time.Time
	publicador                                                                bool
	pressao                                                                   bool
	fila                                                                      *fila.Fila[entrega]
	taxa                                                                      *controle.Adaptativo
	ctx                                                                       context.Context
	cancel                                                                    context.CancelFunc
	recebidas, perdidas                                                       uint64
	descartes                                                                 map[string]uint64
}
type cota struct {
	bucket *controle.Bucket
	visto  time.Time
}
type pendencia struct {
	m          protocol.Mensagem
	addr       *net.UDPAddr
	proxima    time.Time
	tentativas int
}
type resposta struct {
	pedido   protocol.Mensagem
	resposta protocol.Mensagem
}
type saida struct {
	m    protocol.Mensagem
	addr *net.UDPAddr
	par  bool
}
type contadores struct{ recebidas, aceitas, rejeitadas, entregas, descartadas, invalidas, controlePerdido, duplicadas, repasses, bytes atomic.Uint64 }
type Estatisticas struct {
	InteressesRemotos  int    `json:"interesses_remotos"`
	PendenciasControle int    `json:"pendencias_controle"`
	Recebidas          uint64 `json:"recebidas"`
	Aceitas            uint64 `json:"aceitas"`
	Rejeitadas         uint64 `json:"rejeitadas"`
	Entregas           uint64 `json:"entregas_enviadas"`
	Descartadas        uint64 `json:"descartadas"`
	Invalidas          uint64 `json:"invalidas"`
	ControlePerdido    uint64 `json:"controle_sem_confirmacao"`
	Duplicadas         uint64 `json:"duplicadas"`
	Repasses           uint64 `json:"repasses"`
	Bytes              uint64 `json:"bytes_enviados"`
	Clientes           int    `json:"clientes"`
	Topicos            int    `json:"topicos"`
	FilaEntrada        int    `json:"fila_entrada"`
	FilaSaida          int    `json:"fila_saida"`
}
type Broker struct {
	cfg                                                                  Config
	conn                                                                 *net.UDPConn
	par                                                                  *net.UDPAddr
	ctx                                                                  context.Context
	cancel                                                               context.CancelFunc
	wg                                                                   sync.WaitGroup
	uma                                                                  sync.Once
	mu                                                                   sync.Mutex
	topicos                                                              map[string]*topico
	clientes                                                             map[string]*sessao
	cotas                                                                map[string]*cota
	interesses                                                           map[string]time.Time
	pendentes                                                            map[string]*pendencia
	controles                                                            chan saida
	filaPar                                                              *fila.Fila[entrega]
	entrada, saida, controleEntrada, controleSaida, entradaPar, saidaPar *controle.Bucket
	cache                                                                *recente.Cache[resposta]
	vistos                                                               *recente.Cache[bool]
	avisos                                                               *recente.Cache[bool]
	proximo                                                              atomic.Uint64
	cont                                                                 contadores
}

func Iniciar(cfg Config) (*Broker, error) {
	c, e := cfg.padroes()
	if e != nil {
		return nil, e
	}
	addr, e := net.ResolveUDPAddr("udp", c.Endereco)
	if e != nil {
		return nil, e
	}
	conn, e := net.ListenUDP("udp", addr)
	if e != nil {
		return nil, e
	}
	var peer *net.UDPAddr
	if c.Par != "" {
		peer, e = net.ResolveUDPAddr("udp", c.Par)
		if e != nil {
			conn.Close()
			return nil, e
		}
		if peer.String() == conn.LocalAddr().String() {
			conn.Close()
			return nil, errors.New("o par deve ser outro broker")
		}
	}
	if c.ID == "" {
		c.ID = conn.LocalAddr().String()
	}
	if len(c.ID) > protocol.MaxCliente {
		conn.Close()
		return nil, errors.New("ID muito longo")
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{cfg: c, conn: conn, par: peer, ctx: ctx, cancel: cancel, topicos: map[string]*topico{}, clientes: map[string]*sessao{}, cotas: map[string]*cota{}, interesses: map[string]time.Time{}, pendentes: map[string]*pendencia{}, controles: make(chan saida, c.MaxPendencias), filaPar: fila.Nova[entrega](c.FilaTopico, c.BytesTopico), cache: recente.Nova[resposta](c.CacheRespostas, c.ValidadeCache), vistos: recente.Nova[bool](c.CacheVistos, c.ValidadeCache), avisos: recente.Nova[bool](c.CacheAvisos, c.ValidadeCache)}
	b.entrada, _ = controle.Novo(c.TaxaEntrada, c.Rajada)
	b.saida, _ = controle.Novo(c.TaxaSaida, c.Rajada)
	b.controleEntrada, _ = controle.Novo(c.TaxaControle, c.Rajada)
	b.controleSaida, _ = controle.Novo(c.TaxaControle, c.Rajada)
	b.entradaPar, _ = controle.Novo(c.TaxaPar, c.Rajada)
	b.saidaPar, _ = controle.Novo(c.TaxaPar, c.Rajada)
	b.proximo.Store(uint64(time.Now().UnixNano()))
	b.wg.Add(4)
	go b.ler()
	go b.escreverControle()
	go b.manter()
	go b.escreverPar()
	return b, nil
}
func (b *Broker) Endereco() string { return b.conn.LocalAddr().String() }
func (b *Broker) Fechar() error {
	b.uma.Do(func() { b.cancel(); _ = b.conn.Close() })
	b.wg.Wait()
	return nil
}
func chave(m protocol.Mensagem) string           { return m.Cliente + ":" + strconv.FormatUint(m.ID, 10) }
func chaveAviso(a *net.UDPAddr, n uint64) string { return a.String() + ":" + strconv.FormatUint(n, 10) }
func (b *Broker) Estatisticas() Estatisticas {
	s := Estatisticas{Recebidas: b.cont.recebidas.Load(), Aceitas: b.cont.aceitas.Load(), Rejeitadas: b.cont.rejeitadas.Load(), Entregas: b.cont.entregas.Load(), Descartadas: b.cont.descartadas.Load(), Invalidas: b.cont.invalidas.Load(), ControlePerdido: b.cont.controlePerdido.Load(), Duplicadas: b.cont.duplicadas.Load(), Repasses: b.cont.repasses.Load(), Bytes: b.cont.bytes.Load()}
	b.mu.Lock()
	defer b.mu.Unlock()
	s.Clientes = len(b.clientes)
	s.Topicos = len(b.topicos)
	s.InteressesRemotos = len(b.interesses)
	s.PendenciasControle = len(b.pendentes)
	for _, t := range b.topicos {
		n, _ := t.fila.Tamanho()
		s.FilaEntrada += n
	}
	for _, c := range b.clientes {
		n, _ := c.fila.Tamanho()
		s.FilaSaida += n
	}
	return s
}
func (b *Broker) esperar(ctx context.Context, bucket *controle.Bucket) bool {
	t := time.NewTicker(time.Millisecond)
	defer t.Stop()
	for {
		if ctx.Err() != nil {
			return false
		}
		if bucket.Permitir() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
}
func (b *Broker) enviar(ctx context.Context, m protocol.Mensagem, a *net.UDPAddr, classe *controle.Bucket) bool {
	dados, e := protocol.Codificar(m)
	if e != nil {
		b.cont.invalidas.Add(1)
		return false
	}
	if classe != nil && !b.esperar(ctx, classe) {
		return false
	}
	if !b.esperar(ctx, b.saida) {
		return false
	}
	n, e := b.conn.WriteToUDP(dados, a)
	if e == nil {
		b.cont.bytes.Add(uint64(n))
	}
	return e == nil
}
func (b *Broker) controle(m protocol.Mensagem, a *net.UDPAddr, confiavel bool) {
	if confiavel {
		b.mu.Lock()
		if len(b.pendentes) >= b.cfg.MaxPendencias {
			b.mu.Unlock()
			b.cont.controlePerdido.Add(1)
			return
		}
		m.Aviso = b.proximo.Add(1)
		b.pendentes[chaveAviso(a, m.Aviso)] = &pendencia{m: m, addr: a, proxima: time.Now().Add(b.cfg.Repeticao), tentativas: 1}
		b.mu.Unlock()
	}
	select {
	case b.controles <- saida{m: m, addr: a}:
	case <-b.ctx.Done():
	default:
		b.cont.controlePerdido.Add(1)
	}
}
func (b *Broker) escreverControle() {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case s := <-b.controles:
			b.enviar(b.ctx, s.m, s.addr, b.controleSaida)
		}
	}
}
func (b *Broker) confirmar(m protocol.Mensagem, a *net.UDPAddr) {
	b.mu.Lock()
	delete(b.pendentes, chaveAviso(a, m.Aviso))
	b.mu.Unlock()
}
func (b *Broker) aviso(s *sessao, m protocol.Mensagem) {
	m.Origem = b.cfg.ID
	b.controle(m, s.addr, true)
}
func (b *Broker) ler() {
	defer b.wg.Done()
	buf := make([]byte, protocol.MaxMensagem+1)
	for {
		n, a, e := b.conn.ReadFromUDP(buf)
		if e != nil {
			return
		}
		m, e := protocol.Decodificar(buf[:n])
		if e != nil {
			b.cont.invalidas.Add(1)
			continue
		}
		if m.Tipo == protocol.TipoRecebido {
			if b.controleEntrada.Permitir() {
				b.confirmar(m, a)
			}
			continue
		}
		if b.par != nil && a.String() == b.par.String() {
			b.receberPar(m)
			continue
		}
		if m.Tipo != protocol.TipoPublicar && !protocol.Administrativo(m.Tipo) && m.Tipo != protocol.TipoAtividade {
			b.cont.invalidas.Add(1)
			continue
		}
		if m.Cliente == "" || len(m.Cliente) > protocol.MaxCliente || m.Origem != "" {
			b.cont.invalidas.Add(1)
			continue
		}
		if m.Tipo != protocol.TipoPublicar && !b.controleEntrada.Permitir() {
			continue
		}
		s := b.sessao(m.Cliente, a)
		if s == nil {
			continue
		}
		if m.Tipo == protocol.TipoAtividade {
			b.feedback(s, m)
			continue
		}
		if m.ID == 0 {
			continue
		}
		if m.Tipo == protocol.TipoPublicar {
			b.publicar(s, m)
			continue
		}
		b.administrar(s, m)
	}
}
func (b *Broker) sessao(id string, a *net.UDPAddr) *sessao {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx.Err() != nil {
		return nil
	}
	key := a.String()
	if s := b.clientes[key]; s != nil {
		if s.id != id {
			return nil
		}
		s.visto = time.Now()
		if q := b.cotas[id]; q != nil {
			q.visto = s.visto
		}
		return s
	}
	if len(b.clientes) >= b.cfg.MaxClientes || len(b.cotas) >= b.cfg.MaxClientes*2 {
		return nil
	}
	ctx, cancel := context.WithCancel(b.ctx)
	ad, _ := controle.NovoAdaptativo(b.cfg.TaxaCliente)
	s := &sessao{id: id, addr: a, visto: time.Now(), fila: fila.Nova[entrega](b.cfg.FilaCliente, b.cfg.BytesCliente), taxa: ad, ctx: ctx, cancel: cancel, descartes: map[string]uint64{}}
	s.feedbackEm = time.Now()
	b.clientes[key] = s
	if b.cotas[id] == nil {
		q, _ := controle.Novo(b.cfg.TaxaCliente, b.cfg.Rajada)
		b.cotas[id] = &cota{q, time.Now()}
	}
	b.wg.Add(1)
	go b.escreverCliente(s)
	return s
}
func (b *Broker) novoTopico(nome string) *topico {
	if t := b.topicos[nome]; t != nil {
		return t
	}
	if len(b.topicos) >= b.cfg.MaxTopicos {
		return nil
	}
	ctx, cancel := context.WithCancel(b.ctx)
	t := &topico{nome: nome, fila: fila.Nova[entrega](b.cfg.FilaTopico, b.cfg.BytesTopico), inscritos: map[string]*sessao{}, ctx: ctx, cancel: cancel}
	b.topicos[nome] = t
	b.wg.Add(1)
	go b.despachar(t)
	return t
}
func (b *Broker) apagar(nome string) {
	b.mu.Lock()
	t := b.topicos[nome]
	delete(b.topicos, nome)
	delete(b.interesses, nome)
	var inscritos []*sessao
	if t != nil {
		t.cancel()
		t.fila.Limpar()
		for _, s := range t.inscritos {
			inscritos = append(inscritos, s)
			s.fila.Remover(func(e entrega) bool { return e.m.Topico == nome })
		}
	}
	b.mu.Unlock()
	for _, s := range inscritos {
		b.aviso(s, protocol.Mensagem{Tipo: protocol.TipoErro, Topico: nome, Conteudo: []byte("TOPICO_APAGADO")})
	}
}
func (b *Broker) administrar(s *sessao, m protocol.Mensagem) {
	key := s.addr.String() + ":" + chave(m)
	if r, ok := b.cache.Buscar(key); ok {
		if r.pedido.Tipo != m.Tipo || r.pedido.Topico != m.Topico {
			b.controle(protocol.Mensagem{Tipo: protocol.TipoErro, ID: m.ID, Conteudo: []byte("ID_REUTILIZADO")}, s.addr, false)
			return
		}
		b.controle(r.resposta, s.addr, false)
		return
	}
	r := protocol.Mensagem{Tipo: protocol.TipoOK, ID: m.ID, Topico: m.Topico}
	if m.Tipo != protocol.TipoEstatisticas && !protocol.TopicoValido(m.Topico) {
		r.Tipo = protocol.TipoErro
		r.Conteudo = []byte("TOPICO_INVALIDO")
	} else {
		switch m.Tipo {
		case protocol.TipoCriarTopico, protocol.TipoAssinar:
			b.mu.Lock()
			existia := b.topicos[m.Topico] != nil
			t := b.novoTopico(m.Topico)
			if t != nil && m.Tipo == protocol.TipoAssinar {
				t.inscritos[s.addr.String()] = s
			}
			b.mu.Unlock()
			if t == nil {
				r.Tipo = protocol.TipoErro
				r.Conteudo = []byte("LIMITE_TOPICOS")
			} else {
				if m.Tipo == protocol.TipoCriarTopico || !existia {
					b.comunicarTopico(protocol.TipoCriarRemoto, m.Topico)
				}
				b.anunciar(m.Topico)
			}
		case protocol.TipoCancelar:
			b.mu.Lock()
			if t := b.topicos[m.Topico]; t != nil {
				delete(t.inscritos, s.addr.String())
			}
			s.fila.Remover(func(e entrega) bool { return e.m.Topico == m.Topico })
			b.mu.Unlock()
			b.anunciar(m.Topico)
		case protocol.TipoApagarTopico:
			b.apagar(m.Topico)
			b.comunicarTopico(protocol.TipoApagarRemoto, m.Topico)
		case protocol.TipoEstatisticas:
			r.Conteudo, _ = json.Marshal(b.Estatisticas())
		}
	}
	b.cache.Guardar(key, resposta{m, r})
	b.controle(r, s.addr, false)
}
func (b *Broker) publicar(s *sessao, m protocol.Mensagem) {
	b.cont.recebidas.Add(1)
	b.mu.Lock()
	s.publicacoes++
	b.mu.Unlock()
	if !protocol.TopicoValido(m.Topico) {
		b.aviso(s, protocol.Mensagem{Tipo: protocol.TipoErro, ID: m.ID, Conteudo: []byte("TOPICO_INVALIDO")})
		return
	}
	if _, ok := b.vistos.Buscar(chave(m)); ok {
		b.cont.duplicadas.Add(1)
		return
	}
	b.mu.Lock()
	s.publicador = true
	ativos := map[string]bool{}
	for _, c := range b.clientes {
		if c.publicador {
			ativos[c.id] = true
		}
	}
	teto := math.Min(b.cfg.TaxaCliente, b.cfg.TaxaEntrada/float64(len(ativos)))
	q := b.cotas[s.id]
	q.visto = time.Now()
	_ = q.bucket.DefinirTaxa(teto)
	permitido := q.bucket.Permitir() && b.entrada.Permitir()
	if !permitido {
		s.pressao = true
	}
	t := b.topicos[m.Topico]
	interesse, remoto := b.interesses[m.Topico]
	remoto = remoto && time.Since(interesse) < b.cfg.Inatividade
	locais := t != nil && len(t.inscritos) > 0
	b.mu.Unlock()
	if !permitido {
		b.cont.rejeitadas.Add(1)
		b.aviso(s, protocol.Mensagem{Tipo: protocol.TipoErro, ID: m.ID, Conteudo: []byte("RATE_LIMIT")})
		return
	}
	if t == nil {
		b.aviso(s, protocol.Mensagem{Tipo: protocol.TipoErro, ID: m.ID, Topico: m.Topico, Conteudo: []byte("TOPICO_INEXISTENTE")})
		return
	}
	if !locais && !remoto {
		b.aviso(s, protocol.Mensagem{Tipo: protocol.TipoSemInscritos, ID: m.ID, Topico: m.Topico})
		return
	}
	m.Tipo = protocol.TipoMensagem
	m.Origem = b.cfg.ID
	dados, e := protocol.Codificar(m)
	if e != nil {
		b.aviso(s, protocol.Mensagem{Tipo: protocol.TipoErro, ID: m.ID, Conteudo: []byte("MENSAGEM_GRANDE")})
		return
	}
	if _, ok := t.fila.Adicionar(entrega{m, dados}, len(dados), false); !ok {
		b.mu.Lock()
		s.pressao = true
		b.mu.Unlock()
		b.cont.rejeitadas.Add(1)
		b.aviso(s, protocol.Mensagem{Tipo: protocol.TipoErro, ID: m.ID, Conteudo: []byte("TOPICO_SOBRECARREGADO")})
		return
	}
	b.vistos.Guardar(chave(m), true)
	b.cont.aceitas.Add(1)
}
func (b *Broker) despachar(t *topico) {
	defer b.wg.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-t.fila.Sinal:
		}
		for {
			e, ok := t.fila.Retirar()
			if !ok {
				break
			}
			b.mu.Lock()
			if b.topicos[t.nome] != t {
				b.mu.Unlock()
				break
			}
			var dest []*sessao
			for _, s := range t.inscritos {
				dest = append(dest, s)
			}
			v, remoto := b.interesses[t.nome]
			remoto = remoto && time.Since(v) < b.cfg.Inatividade
			b.mu.Unlock()
			for _, s := range dest {
				removidos, _ := s.fila.Adicionar(e, len(e.dados), true)
				if len(removidos) > 0 {
					b.cont.descartadas.Add(uint64(len(removidos)))
					b.mu.Lock()
					for _, r := range removidos {
						s.descartes[r.m.Topico]++
					}
					b.mu.Unlock()
				}
			}
			if e.m.Tipo != protocol.TipoRepasse && remoto && b.par != nil {
				m := e.m
				m.Tipo = protocol.TipoRepasse
				dados, err := protocol.Codificar(m)
				if err == nil {
					_, ok = b.filaPar.Adicionar(entrega{m, dados}, len(dados), false)
					if !ok {
						b.cont.descartadas.Add(1)
					}
				}
			}
		}
	}
}
func (b *Broker) escreverCliente(s *sessao) {
	defer b.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.fila.Sinal:
		}
		for {
			e, ok := s.fila.Retirar()
			if !ok {
				break
			}
			b.mu.Lock()
			s.emEnvio = true
			b.mu.Unlock()
			if s.taxa.Aguardar(s.ctx) != nil {
				return
			}
			b.mu.Lock()
			t := b.topicos[e.m.Topico]
			s.emEnvio = false
			valido := t != nil && t.inscritos[s.addr.String()] == s
			b.mu.Unlock()
			if !valido {
				continue
			}
			m := e.m
			m.Tipo = protocol.TipoMensagem
			if b.enviar(s.ctx, m, s.addr, nil) {
				b.cont.entregas.Add(1)
				b.mu.Lock()
				s.envios++
				b.mu.Unlock()
			}
		}
	}
}
func (b *Broker) escreverPar() {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.filaPar.Sinal:
		}
		for {
			e, ok := b.filaPar.Retirar()
			if !ok {
				break
			}
			if b.par != nil && b.enviar(b.ctx, e.m, b.par, b.saidaPar) {
				b.cont.repasses.Add(1)
			}
		}
	}
}
func (b *Broker) feedback(s *sessao, m protocol.Mensagem) {
	b.mu.Lock()
	if !s.feedbackInicial {
		s.recebidasBase = m.Recebidas
		s.enviadasBase = m.Enviadas
		s.feedbackInicial = true
	}
	recebidas := uint64(0)
	if m.Recebidas >= s.recebidasBase {
		recebidas = m.Recebidas - s.recebidasBase
	}
	gap := uint64(0)
	if s.envios > recebidas {
		gap = s.envios - recebidas
	}
	if gap > s.gapAnterior {
		s.perdasSeguidas++
	} else {
		s.perdasSeguidas = 0
	}
	s.gapAnterior = gap
	enviadas := uint64(0)
	if m.Enviadas >= s.enviadasBase {
		enviadas = m.Enviadas - s.enviadasBase
	}
	gapIn := uint64(0)
	if enviadas > s.publicacoes {
		gapIn = enviadas - s.publicacoes
	}
	if gapIn > s.gapEntrada {
		s.perdasEntrada++
	} else {
		s.perdasEntrada = 0
	}
	s.gapEntrada = gapIn
	if s.perdasEntrada >= 2 {
		s.pressao = true
	}
	demandaRecepcao := m.Recebidas > s.recebidas || s.emEnvio
	qfila, _ := s.fila.Tamanho()
	demandaRecepcao = demandaRecepcao || qfila > 0
	s.feedbackEm = time.Now()
	perda := m.Perdidas > s.perdidas || s.perdasSeguidas >= 2
	s.recebidas = m.Recebidas
	s.perdidas = m.Perdidas
	n := 0
	pubs := map[string]bool{}
	for _, c := range b.clientes {
		if c.publicador {
			pubs[c.id] = true
		}
		for _, t := range b.topicos {
			if t.inscritos[c.addr.String()] != nil {
				n++
				break
			}
		}
	}
	if b.par != nil {
		n++
	}
	if n < 1 {
		n = 1
	}
	teto := math.Min(b.cfg.TaxaCliente, b.cfg.TaxaSaida*.8/float64(n))
	np := len(pubs)
	if np < 1 {
		np = 1
	}
	entrada := math.Min(b.cfg.TaxaCliente, b.cfg.TaxaEntrada/float64(np))
	pressao := 0
	if s.pressao {
		pressao = 100
	}
	for _, t := range b.topicos {
		q, _ := t.fila.Tamanho()
		if q*2 >= b.cfg.FilaTopico {
			pressao = 100
		}
	}
	s.pressao = false
	b.mu.Unlock()
	s.taxa.AvaliarComDemanda(m.Fila >= 50 || perda, teto, demandaRecepcao)
	b.controle(protocol.Mensagem{Tipo: protocol.TipoEstado, ID: m.ID, Taxa: entrada, Fila: pressao}, s.addr, false)
}
func (b *Broker) String() string { return fmt.Sprintf("broker %s UDP %s", b.cfg.ID, b.Endereco()) }
