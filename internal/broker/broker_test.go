package broker_test

import (
	"context"
	"fmt"
	"github.com/Luizalima3/middleware-pubsub-go/client"
	"github.com/Luizalima3/middleware-pubsub-go/internal/broker"
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func iniciar(t *testing.T, c broker.Config) *broker.Broker {
	t.Helper()
	if c.Endereco == "" {
		c.Endereco = "127.0.0.1:0"
	}
	if c.TaxaCliente == 0 {
		c.TaxaCliente = 1000
	}
	if c.Rajada == 0 {
		c.Rajada = 16
	}
	c.Intervalo = 30 * time.Millisecond
	c.Inatividade = 600 * time.Millisecond
	c.Repeticao = 40 * time.Millisecond
	b, e := broker.Iniciar(c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { b.Fechar() })
	return b
}
func conectar(t *testing.T, ends []string, buffer int) *client.Client {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	c, e := client.ConectarComConfig(ctx, client.Config{Brokers: ends, Buffer: buffer, TaxaMaxima: 1000, Intervalo: 30 * time.Millisecond, Timeout: 180 * time.Millisecond, Repeticao: 40 * time.Millisecond, Tentativas: 3})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Fechar() })
	return c
}
func prazo(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 2*time.Second)
}
func esperar(t *testing.T, cond func() bool) {
	t.Helper()
	fim := time.Now().Add(3 * time.Second)
	for time.Now().Before(fim) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condição não ocorreu dentro do prazo")
}
func receber(t *testing.T, c *client.Client, tipo protocol.Tipo) protocol.Mensagem {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case m, ok := <-c.Eventos():
			if !ok {
				t.Fatal("cliente fechado")
			}
			if m.Tipo == tipo {
				return m
			}
		case <-timer.C:
			t.Fatalf("não recebeu %s", tipo)
		}
	}
}
func TestPublicarAssinarCancelarApagarENotificar(t *testing.T) {
	b := iniciar(t, broker.Config{})
	c := conectar(t, []string{b.Endereco()}, 8)
	p := conectar(t, []string{b.Endereco()}, 8)
	ctx, end := prazo(t)
	defer end()
	if _, e := c.Assinar(ctx, "jogo"); e != nil {
		t.Fatal(e)
	}
	if _, e := p.Publicar(ctx, "jogo", []byte("Gol!")); e != nil {
		t.Fatal(e)
	}
	if m := receber(t, c, protocol.TipoMensagem); string(m.Conteudo) != "Gol!" {
		t.Fatal(m)
	}
	if _, e := c.Cancelar(ctx, "jogo"); e != nil {
		t.Fatal(e)
	}
	id, e := p.Publicar(ctx, "jogo", []byte("sem inscritos"))
	if e != nil {
		t.Fatal(e)
	}
	if m := receber(t, p, protocol.TipoSemInscritos); m.ID != id || m.Aviso == 0 {
		t.Fatal(m)
	}
	if _, e := c.Assinar(ctx, "jogo"); e != nil {
		t.Fatal(e)
	}
	if _, e := p.ApagarTopico(ctx, "jogo"); e != nil {
		t.Fatal(e)
	}
	if m := receber(t, c, protocol.TipoErro); string(m.Conteudo) != "TOPICO_APAGADO" {
		t.Fatal(m)
	}
	esperar(t, func() bool { return b.Estatisticas().Topicos == 0 })
}
func par(t *testing.T) (*broker.Broker, *broker.Broker) {
	t.Helper()
	res, e := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if e != nil {
		t.Fatal(e)
	}
	end := res.LocalAddr().String()
	res.Close()
	b1 := iniciar(t, broker.Config{Par: end})
	b2 := iniciar(t, broker.Config{Endereco: end, Par: b1.Endereco()})
	return b1, b2
}
func TestDoisBrokersRepasseSemCicloETopicos(t *testing.T) {
	b1, b2 := par(t)
	s := conectar(t, []string{b1.Endereco()}, 8)
	p := conectar(t, []string{b2.Endereco()}, 8)
	ctx, end := prazo(t)
	defer end()
	if _, e := s.Assinar(ctx, "evento"); e != nil {
		t.Fatal(e)
	}
	esperar(t, func() bool { return b2.Estatisticas().InteressesRemotos == 1 })
	if _, e := p.Publicar(ctx, "evento", []byte("entre brokers")); e != nil {
		t.Fatal(e)
	}
	receber(t, s, protocol.TipoMensagem)
	time.Sleep(100 * time.Millisecond)
	if b2.Estatisticas().Repasses != 1 || b1.Estatisticas().Repasses != 0 {
		t.Fatalf("repasse circular: %+v %+v", b1.Estatisticas(), b2.Estatisticas())
	}
	if _, e := s.Cancelar(ctx, "evento"); e != nil {
		t.Fatal(e)
	}
	esperar(t, func() bool { return b2.Estatisticas().InteressesRemotos == 0 })
	if _, e := p.Publicar(ctx, "evento", []byte("vazio")); e != nil {
		t.Fatal(e)
	}
	receber(t, p, protocol.TipoSemInscritos)
	if _, e := p.ApagarTopico(ctx, "evento"); e != nil {
		t.Fatal(e)
	}
	esperar(t, func() bool { return b1.Estatisticas().Topicos == 0 && b2.Estatisticas().Topicos == 0 })
}
func TestFailoverReenviaInscricoes(t *testing.T) {
	b1, b2 := par(t)
	s := conectar(t, []string{b1.Endereco(), b2.Endereco()}, 8)
	p := conectar(t, []string{b1.Endereco(), b2.Endereco()}, 8)
	ctx, end := prazo(t)
	defer end()
	if _, e := s.Assinar(ctx, "recuperacao"); e != nil {
		t.Fatal(e)
	}
	b1.Fechar()
	esperar(t, func() bool { return s.BrokerAtual() == b2.Endereco() && p.BrokerAtual() == b2.Endereco() })
	ctx2, end2 := prazo(t)
	defer end2()
	if _, e := p.Publicar(ctx2, "recuperacao", []byte("voltou")); e != nil {
		t.Fatal(e)
	}
	if m := receber(t, s, protocol.TipoMensagem); string(m.Conteudo) != "voltou" {
		t.Fatal(m)
	}
}
func TestConsumidorLentoNaoBloqueiaRapido(t *testing.T) {
	b := iniciar(t, broker.Config{FilaCliente: 4, BytesCliente: 4000, TaxaCliente: 1000, Rajada: 32})
	lento := conectar(t, []string{b.Endereco()}, 2)
	rapido := conectar(t, []string{b.Endereco()}, 32)
	p := conectar(t, []string{b.Endereco()}, 8)
	ctx, end := context.WithTimeout(context.Background(), 3*time.Second)
	defer end()
	if _, e := lento.Assinar(ctx, "teste"); e != nil {
		t.Fatal(e)
	}
	if _, e := rapido.Assinar(ctx, "teste"); e != nil {
		t.Fatal(e)
	}
	var nr, nl atomic.Int64
	fim := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-fim:
				return
			case m := <-rapido.Eventos():
				if m.Tipo == protocol.TipoMensagem {
					nr.Add(1)
				}
			}
		}
	}()
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		for {
			select {
			case <-fim:
				return
			case m := <-lento.Eventos():
				if m.Tipo == protocol.TipoMensagem {
					nl.Add(1)
					time.Sleep(60 * time.Millisecond)
				}
			}
		}
	}()
	for i := 0; i < 80; i++ {
		if _, e := p.Publicar(ctx, "teste", []byte(fmt.Sprint(i))); e != nil {
			t.Fatal(e)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	close(fim)
	<-done
	<-done2
	if nr.Load() < 50 || nr.Load() <= nl.Load() {
		t.Fatalf("rápido=%d lento=%d", nr.Load(), nl.Load())
	}
	if b.Estatisticas().Descartadas == 0 {
		t.Fatalf("não exerceu pressão: %+v", b.Estatisticas())
	}
}
func socket(t *testing.T) *net.UDPConn {
	t.Helper()
	c, e := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func enviar(t *testing.T, c *net.UDPConn, a string, m protocol.Mensagem) {
	t.Helper()
	addr, e := net.ResolveUDPAddr("udp", a)
	if e != nil {
		t.Fatal(e)
	}
	d, e := protocol.Codificar(m)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.WriteToUDP(d, addr); e != nil {
		t.Fatal(e)
	}
}
func ler(t *testing.T, c *net.UDPConn, tipo protocol.Tipo) protocol.Mensagem {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1001)
	for {
		n, _, e := c.ReadFromUDP(buf)
		if e != nil {
			t.Fatal(e)
		}
		m, e := protocol.Decodificar(buf[:n])
		if e == nil && m.Tipo == tipo {
			return m
		}
	}
}
func TestPedidoRepetidoNaoApagaTopicoRecriado(t *testing.T) {
	b := iniciar(t, broker.Config{})
	c := socket(t)
	op := func(tipo protocol.Tipo, id uint64) {
		enviar(t, c, b.Endereco(), protocol.Mensagem{Tipo: tipo, ID: id, Cliente: "cliente", Topico: "x"})
		ler(t, c, protocol.TipoOK)
	}
	op(protocol.TipoCriarTopico, 1)
	op(protocol.TipoApagarTopico, 2)
	op(protocol.TipoCriarTopico, 3)
	op(protocol.TipoApagarTopico, 2)
	if b.Estatisticas().Topicos != 1 {
		t.Fatal("executou novamente um pedido já confirmado")
	}
	enviar(t, c, b.Endereco(), protocol.Mensagem{Tipo: protocol.TipoApagarTopico, ID: 2, Cliente: "cliente", Topico: "outro"})
	if m := ler(t, c, protocol.TipoErro); string(m.Conteudo) != "ID_REUTILIZADO" {
		t.Fatal(m)
	}
}
func TestAvisoSemInscritosRepeteAteConfirmacao(t *testing.T) {
	b := iniciar(t, broker.Config{})
	c := socket(t)
	enviar(t, c, b.Endereco(), protocol.Mensagem{Tipo: protocol.TipoCriarTopico, ID: 1, Cliente: "ana", Topico: "vazio"})
	ler(t, c, protocol.TipoOK)
	enviar(t, c, b.Endereco(), protocol.Mensagem{Tipo: protocol.TipoPublicar, ID: 2, Cliente: "ana", Topico: "vazio", Conteudo: []byte("oi")})
	a := ler(t, c, protocol.TipoSemInscritos)
	r := ler(t, c, protocol.TipoSemInscritos)
	if a.Aviso == 0 || a.Aviso != r.Aviso {
		t.Fatal(a, r)
	}
	enviar(t, c, b.Endereco(), protocol.Mensagem{Tipo: protocol.TipoRecebido, Aviso: a.Aviso})
	esperar(t, func() bool { return b.Estatisticas().PendenciasControle == 0 })
}
func TestTetoGlobalEPorIdentificador(t *testing.T) {
	b := iniciar(t, broker.Config{TaxaCliente: 20, TaxaEntrada: 25, Rajada: 2})
	s := conectar(t, []string{b.Endereco()}, 32)
	ctx, end := prazo(t)
	defer end()
	s.Assinar(ctx, "x")
	a := socket(t)
	c := socket(t)
	inicio := time.Now()
	for i := 1; i <= 100; i++ {
		sock := a
		if i%2 == 0 {
			sock = c
		}
		enviar(t, sock, b.Endereco(), protocol.Mensagem{Tipo: protocol.TipoPublicar, ID: uint64(i), Cliente: "mesmo-id", Topico: "x", Conteudo: []byte("x")})
	}
	esperar(t, func() bool { return b.Estatisticas().Recebidas >= 100 })
	st := b.Estatisticas()
	limite := uint64(2+20*time.Since(inicio).Seconds()) + 1
	if st.Aceitas > limite || st.Rejeitadas == 0 {
		t.Fatalf("limite=%d %+v", limite, st)
	}
}
func TestMensagemExcessivaNaoCriaSessao(t *testing.T) {
	b := iniciar(t, broker.Config{})
	c := socket(t)
	a, _ := net.ResolveUDPAddr("udp", b.Endereco())
	c.WriteToUDP([]byte(strings.Repeat("x", 1001)), a)
	esperar(t, func() bool { return b.Estatisticas().Invalidas > 0 })
	if b.Estatisticas().Clientes != 0 {
		t.Fatal("criou sessão inválida")
	}
}
func TestRepeticaoAposPerdaDeConfirmacao(t *testing.T) {
	b := iniciar(t, broker.Config{})
	proxy := socket(t)
	dest, _ := net.ResolveUDPAddr("udp", b.Endereco())
	var descartou atomic.Bool
	var bloquearTodas atomic.Bool
	var pedidos atomic.Int64
	pronto := make(chan struct{})
	go func() {
		defer close(pronto)
		buf := make([]byte, 1001)
		var origem *net.UDPAddr
		for {
			n, a, e := proxy.ReadFromUDP(buf)
			if e != nil {
				return
			}
			d := append([]byte(nil), buf[:n]...)
			m, _ := protocol.Decodificar(d)
			if a.String() == dest.String() {
				if m.Tipo == protocol.TipoOK && bloquearTodas.Load() {
					continue
				}
				if m.Tipo == protocol.TipoOK && descartou.CompareAndSwap(false, true) {
					continue
				}
				if origem != nil {
					proxy.WriteToUDP(d, origem)
				}
			} else {
				origem = a
				if m.Tipo == protocol.TipoCriarTopico {
					pedidos.Add(1)
				}
				proxy.WriteToUDP(d, dest)
			}
		}
	}()
	c := conectar(t, []string{proxy.LocalAddr().String()}, 8)
	ctx, end := prazo(t)
	defer end()
	if _, e := c.CriarTopico(ctx, "repetido"); e != nil {
		t.Fatal(e)
	}
	if !descartou.Load() || pedidos.Load() < 2 || b.Estatisticas().Topicos != 1 {
		t.Fatal("não confirmou a repetição segura")
	}
	bloquearTodas.Store(true)
	if _, err := c.CriarTopico(ctx, "sem-resposta"); err == nil || !strings.Contains(err.Error(), "desconhecido") {
		t.Fatalf("deveria informar resultado desconhecido: %v", err)
	}
	if b.Estatisticas().Topicos != 2 {
		t.Fatal("o teste deveria executar a operação e perder apenas a confirmação")
	}
	c.Fechar()
	proxy.Close()
	<-pronto
}

func TestTaxaMantidaSemPressaoEReduzidaComExcesso(t *testing.T) {
	b := iniciar(t, broker.Config{TaxaCliente: 50, Rajada: 8})
	s := conectar(t, []string{b.Endereco()}, 64)
	ctx, end := context.WithTimeout(context.Background(), 3*time.Second)
	defer end()
	if _, e := s.Assinar(ctx, "adaptacao"); e != nil {
		t.Fatal(e)
	}
	p, e := client.ConectarComConfig(ctx, client.Config{Brokers: []string{b.Endereco()}, TaxaMaxima: 50, IgnorarControle: true, Intervalo: 30 * time.Millisecond, Timeout: 180 * time.Millisecond, Repeticao: 40 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer p.Fechar()
	time.Sleep(80 * time.Millisecond)
	if p.Taxa() != 50 {
		t.Fatalf("reduziu sem pressão: %.2f", p.Taxa())
	}
	for i := 0; i < 60; i++ {
		if _, e = p.Publicar(ctx, "adaptacao", []byte("teste")); e != nil {
			t.Fatal(e)
		}
	}
	esperar(t, func() bool { return p.Taxa() < 50 })
	if b.Estatisticas().Rejeitadas == 0 {
		t.Fatal("não houve excesso para acionar redução")
	}
}
