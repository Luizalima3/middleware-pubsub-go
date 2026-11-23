package client

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/broker"
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
)

type udpTestBroker struct {
	conn *net.UDPConn
	done chan struct{}
	once sync.Once
}

func newUDPTestBroker(t *testing.T, handle func(*udpTestBroker, protocol.Mensagem, *net.UDPAddr)) *udpTestBroker {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	b := &udpTestBroker{conn: conn, done: make(chan struct{})}
	t.Cleanup(func() { b.close() })

	go func() {
		defer close(b.done)
		buf := make([]byte, protocol.MaxMensagem+1)
		for {
			n, addr, err := b.conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			m, err := protocol.Decodificar(buf[:n])
			if err == nil && handle != nil {
				handle(b, m, addr)
			}
		}
	}()
	return b
}

func (b *udpTestBroker) address() string {
	return b.conn.LocalAddr().String()
}

func (b *udpTestBroker) send(addr *net.UDPAddr, m protocol.Mensagem) error {
	data, err := protocol.Codificar(m)
	if err != nil {
		return err
	}
	_, err = b.conn.WriteToUDP(data, addr)
	return err
}

func (b *udpTestBroker) close() {
	b.once.Do(func() { _ = b.conn.Close() })
	<-b.done
}

func respondToClient(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
	switch m.Tipo {
	case protocol.TipoAtividade:
		_ = b.send(addr, protocol.Mensagem{Tipo: protocol.TipoEstado, ID: m.ID, Taxa: 100})
	case protocol.TipoEstatisticas:
		_ = b.send(addr, protocol.Mensagem{
			Tipo:     protocol.TipoOK,
			ID:       m.ID,
			Conteudo: []byte(`{"topicos":2}`),
		})
	default:
		if protocol.Administrativo(m.Tipo) {
			_ = b.send(addr, protocol.Mensagem{Tipo: protocol.TipoOK, ID: m.ID, Topico: m.Topico})
		}
	}
}

func configForBrokers(brokers ...*udpTestBroker) Config {
	addresses := make([]string, len(brokers))
	for i, b := range brokers {
		addresses[i] = b.address()
	}
	return Config{
		Brokers:    addresses,
		TaxaMaxima: 100,
		Intervalo:  20 * time.Millisecond,
		Timeout:    120 * time.Millisecond,
		Repeticao:  20 * time.Millisecond,
		Tentativas: 3,
	}
}

func connectTestClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := ConectarComConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Fechar() })
	return c
}

func receiveEvent(t *testing.T, c *Client, kind protocol.Tipo) protocol.Mensagem {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case m, ok := <-c.Eventos():
			if !ok {
				t.Fatal("canal de eventos fechado")
			}
			if m.Tipo == kind {
				return m
			}
		case <-timer.C:
			t.Fatalf("não recebeu evento %s", kind)
		}
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condição não satisfeita dentro do prazo")
}

func TestRespostasJSONOKEERROInterpretadas(t *testing.T) {
	b := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoCriarTopico {
			_ = b.send(addr, protocol.Mensagem{
				Tipo:     protocol.TipoErro,
				ID:       m.ID,
				Topico:   m.Topico,
				Conteudo: []byte("TOPICO_INVALIDO"),
			})
			return
		}
		respondToClient(b, m, addr)
	})
	c := connectTestClient(t, configForBrokers(b))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := c.Estatisticas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if response.Tipo != protocol.TipoOK || response.ID == 0 {
		t.Fatalf("resposta inesperada: %+v", response)
	}
	if got, want := string(response.Conteudo), `{"topicos":2}`; got != want {
		t.Fatalf("conteúdo JSON = %q; esperado %q", got, want)
	}
	if _, err := c.CriarTopico(ctx, "rejeitado"); err == nil || !strings.Contains(err.Error(), "broker: TOPICO_INVALIDO") {
		t.Fatalf("erro do broker não interpretado: %v", err)
	}
}

func TestRespostaAssociadaAoIDEEEndpointCorretos(t *testing.T) {
	other := newUDPTestBroker(t, respondToClient)
	first := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoAtividade {
			respondToClient(b, m, addr)
			return
		}
		if m.Tipo != protocol.TipoEstatisticas {
			return
		}
		_ = b.send(addr, protocol.Mensagem{Tipo: protocol.TipoEstado, ID: m.ID, Taxa: 100})
		_ = b.send(addr, protocol.Mensagem{Tipo: protocol.TipoOK, ID: m.ID + 1, Conteudo: []byte("id incorreto")})
		_ = other.send(addr, protocol.Mensagem{Tipo: protocol.TipoOK, ID: m.ID, Conteudo: []byte("broker incorreto")})
		time.Sleep(15 * time.Millisecond)
		_ = b.send(addr, protocol.Mensagem{Tipo: protocol.TipoOK, ID: m.ID, Conteudo: []byte(`{"correto":true}`)})
	})
	cfg := configForBrokers(first, other)
	cfg.Repeticao = 200 * time.Millisecond
	c := connectTestClient(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := c.Estatisticas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(response.Conteudo), `{"correto":true}`; got != want {
		t.Fatalf("resposta aceita = %q; esperada %q", got, want)
	}
}

func TestPedidoAdministrativoRepeteComMesmoIDELimite(t *testing.T) {
	requests := make(chan protocol.Mensagem, 8)
	b := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoAtividade {
			respondToClient(b, m, addr)
			return
		}
		if m.Tipo == protocol.TipoCriarTopico {
			requests <- m
		}
	})
	cfg := configForBrokers(b)
	cfg.Repeticao = 15 * time.Millisecond
	cfg.Tentativas = 3
	c := connectTestClient(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.CriarTopico(ctx, "repeticao")
	if err == nil || !strings.Contains(err.Error(), "resultado da operação é desconhecido") {
		t.Fatalf("erro de confirmação esperado, recebido: %v", err)
	}

	var received []protocol.Mensagem
	for i := 0; i < cfg.Tentativas; i++ {
		select {
		case m := <-requests:
			received = append(received, m)
		case <-time.After(time.Second):
			t.Fatalf("recebeu %d tentativas; esperado %d", len(received), cfg.Tentativas)
		}
	}
	if len(received) != cfg.Tentativas {
		t.Fatalf("número de tentativas = %d; esperado %d", len(received), cfg.Tentativas)
	}
	for _, m := range received {
		if m.ID != received[0].ID || m.Tipo != protocol.TipoCriarTopico || m.Topico != "repeticao" {
			t.Fatalf("pedido mudou entre tentativas: %+v; primeiro %+v", m, received[0])
		}
	}
}

func TestFailoverNaoReenviaComandoPendenteAoSegundoBroker(t *testing.T) {
	var comandoPendente atomic.Bool
	requests := make(chan protocol.Mensagem, 8)
	first := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoAtividade {
			if !comandoPendente.Load() {
				respondToClient(b, m, addr)
			}
			return
		}
		if m.Tipo == protocol.TipoCriarTopico {
			comandoPendente.Store(true)
			requests <- m
		}
	})
	secondRequests := make(chan protocol.Mensagem, 8)
	second := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoCriarTopico {
			secondRequests <- m
		}
		respondToClient(b, m, addr)
	})
	cfg := configForBrokers(first, second)
	cfg.Timeout = 100 * time.Millisecond
	cfg.Repeticao = 80 * time.Millisecond
	c := connectTestClient(t, cfg)

	result := make(chan error, 1)
	go func() {
		_, err := c.CriarTopico(context.Background(), "pendente")
		result <- err
	}()

	var firstAttempts []protocol.Mensagem
	for len(firstAttempts) < cfg.Tentativas {
		select {
		case m := <-requests:
			firstAttempts = append(firstAttempts, m)
		case <-time.After(2 * time.Second):
			t.Fatalf("tentativas recebidas: %d; esperado %d", len(firstAttempts), cfg.Tentativas)
		}
	}
	err := <-result
	if !errors.Is(err, ErrResultadoDesconhecido) {
		t.Fatalf("resultado pendente deveria ser desconhecido: %v", err)
	}
	for _, m := range firstAttempts {
		if m.ID != firstAttempts[0].ID {
			t.Fatalf("ID mudou durante repetição: %v", firstAttempts)
		}
	}
	waitFor(t, func() bool { return c.BrokerAtual() == second.address() })
	select {
	case m := <-secondRequests:
		t.Fatalf("pedido pendente foi reenviado ao segundo broker: %+v", m)
	case <-time.After(60 * time.Millisecond):
	}
}

func TestCancelamentoSemConfirmacaoNaoERecriadoNoFailover(t *testing.T) {
	var cancelamentoPendente atomic.Bool
	cancelRequests := make(chan protocol.Mensagem, 8)
	first := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		switch m.Tipo {
		case protocol.TipoAtividade:
			if !cancelamentoPendente.Load() {
				respondToClient(b, m, addr)
			}
		case protocol.TipoAssinar:
			respondToClient(b, m, addr)
		case protocol.TipoCancelar:
			cancelamentoPendente.Store(true)
			cancelRequests <- m
		}
	})
	recovered := make(chan string, 8)
	second := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoAssinar {
			recovered <- m.Topico
		}
		respondToClient(b, m, addr)
	})
	cfg := configForBrokers(first, second)
	cfg.Timeout = 100 * time.Millisecond
	cfg.Repeticao = 50 * time.Millisecond
	c := connectTestClient(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.Assinar(ctx, "cancelamento-incerto"); err != nil {
		t.Fatal(err)
	}
	receiveEvent(t, c, protocol.TipoOK)

	result := make(chan error, 1)
	go func() {
		_, err := c.Cancelar(context.Background(), "cancelamento-incerto")
		result <- err
	}()
	select {
	case <-cancelRequests:
	case <-time.After(time.Second):
		t.Fatal("o broker de teste não recebeu CANCELAR")
	}
	err := <-result
	if !errors.Is(err, ErrResultadoDesconhecido) {
		t.Fatalf("resultado do cancelamento deveria ser desconhecido: %v", err)
	}
	waitFor(t, func() bool { return c.BrokerAtual() == second.address() })
	select {
	case topic := <-recovered:
		t.Fatalf("inscrição com cancelamento incerto foi restaurada: %s", topic)
	case <-time.After(60 * time.Millisecond):
	}
}

func TestEntregaDeMensagemEDescarteDeDuplicata(t *testing.T) {
	var clientAddrMu sync.Mutex
	var clientAddr *net.UDPAddr
	b := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoAssinar {
			clientAddrMu.Lock()
			clientAddr = addr
			clientAddrMu.Unlock()
		}
		if m.Tipo == protocol.TipoAtividade || protocol.Administrativo(m.Tipo) {
			respondToClient(b, m, addr)
		}
	})
	c := connectTestClient(t, configForBrokers(b))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Assinar(ctx, "entrega"); err != nil {
		t.Fatal(err)
	}
	receiveEvent(t, c, protocol.TipoOK)

	clientAddrMu.Lock()
	addr := clientAddr
	clientAddrMu.Unlock()
	if addr == nil {
		t.Fatal("o broker de teste não observou ASSINAR")
	}
	message := protocol.Mensagem{
		Tipo:     protocol.TipoMensagem,
		ID:       77,
		Cliente:  "publicador",
		Topico:   "entrega",
		Conteudo: []byte("ação recebida"),
	}
	if err := b.send(addr, message); err != nil {
		t.Fatal(err)
	}
	if err := b.send(addr, message); err != nil {
		t.Fatal(err)
	}

	got := receiveEvent(t, c, protocol.TipoMensagem)
	if got.ID != message.ID || got.Topico != message.Topico || string(got.Conteudo) != string(message.Conteudo) {
		t.Fatalf("mensagem recebida incorreta: %+v", got)
	}
	waitFor(t, func() bool { return c.recebidas.Load() == 1 })
	select {
	case m := <-c.Eventos():
		if m.Tipo == protocol.TipoMensagem {
			t.Fatalf("duplicata entregue: %+v", m)
		}
	case <-time.After(60 * time.Millisecond):
	}
}

func TestFailoverReconstruiInscricoesAtivasEExcluiCanceladas(t *testing.T) {
	recovered := make(chan string, 16)
	first := newUDPTestBroker(t, respondToClient)
	second := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoAssinar {
			recovered <- m.Topico
		}
		respondToClient(b, m, addr)
	})
	cfg := configForBrokers(first, second)
	cfg.Timeout = 120 * time.Millisecond
	c := connectTestClient(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, topic := range []string{"mantem/a", "mantem/b", "cancelada", "apagada"} {
		if _, err := c.Assinar(ctx, topic); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Cancelar(ctx, "cancelada"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApagarTopico(ctx, "apagada"); err != nil {
		t.Fatal(err)
	}

	first.close()
	waitFor(t, func() bool { return c.BrokerAtual() == second.address() })

	var got []string
	for len(got) < 2 {
		select {
		case topic := <-recovered:
			got = append(got, topic)
		case <-time.After(time.Second):
			t.Fatalf("inscrições reconstruídas: %v", got)
		}
	}
	if !reflect.DeepEqual(stringSet(got), stringSet([]string{"mantem/a", "mantem/b"})) {
		t.Fatalf("inscrições reconstruídas = %v", got)
	}
	select {
	case topic := <-recovered:
		t.Fatalf("inscrição cancelada ou apagada foi reconstruída: %s", topic)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestFailoverEntreBrokersIndependentesPermiteNovaEntrega(t *testing.T) {
	first := iniciarBrokerIndependente(t)
	second := iniciarBrokerIndependente(t)
	cfg := Config{
		Brokers:    []string{first.Endereco(), second.Endereco()},
		TaxaMaxima: 1000,
		Buffer:     8,
		Intervalo:  20 * time.Millisecond,
		Timeout:    120 * time.Millisecond,
		Repeticao:  20 * time.Millisecond,
		Tentativas: 3,
	}
	subscriber := connectTestClient(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if _, err := subscriber.Assinar(ctx, "independente/failover"); err != nil {
		t.Fatal(err)
	}
	receiveEvent(t, subscriber, protocol.TipoOK)

	if err := first.Fechar(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return subscriber.BrokerAtual() == second.Endereco() })
	if got := second.Estatisticas().Topicos; got != 1 {
		t.Fatalf("o segundo broker não criou o tópico pela reinscrição: tópicos=%d", got)
	}

	publisher := connectTestClient(t, Config{
		Brokers:    []string{second.Endereco()},
		TaxaMaxima: 1000,
		Buffer:     8,
		Intervalo:  20 * time.Millisecond,
		Timeout:    120 * time.Millisecond,
		Repeticao:  20 * time.Millisecond,
		Tentativas: 3,
	})
	id, err := publisher.Publicar(ctx, "independente/failover", []byte("após failover"))
	if err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case m := <-subscriber.Eventos():
			if m.Tipo == protocol.TipoMensagem && m.ID == id {
				if string(m.Conteudo) != "após failover" {
					t.Fatalf("conteúdo recebido incorreto: %q", m.Conteudo)
				}
				return
			}
		case <-timer.C:
			t.Fatal("não recebeu publicação depois do failover")
		}
	}
}

func iniciarBrokerIndependente(t *testing.T) *broker.Broker {
	t.Helper()
	b, err := broker.Iniciar(broker.Config{
		Endereco:    "127.0.0.1:0",
		Intervalo:   20 * time.Millisecond,
		Inatividade: 600 * time.Millisecond,
		Repeticao:   20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Fechar() })
	return b
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func TestConectarUsaSegundoBrokerQuandoPrimeiroIndisponivel(t *testing.T) {
	first := newUDPTestBroker(t, nil)
	second := newUDPTestBroker(t, respondToClient)
	cfg := configForBrokers(first, second)
	cfg.Repeticao = 15 * time.Millisecond
	cfg.Tentativas = 2
	c := connectTestClient(t, cfg)

	if got, want := c.BrokerAtual(), second.address(); got != want {
		t.Fatalf("broker atual = %s; esperado %s", got, want)
	}
}

func TestConectarFalhaComDoisBrokersIndisponiveis(t *testing.T) {
	first := newUDPTestBroker(t, nil)
	second := newUDPTestBroker(t, nil)
	cfg := configForBrokers(first, second)
	cfg.Repeticao = 10 * time.Millisecond
	cfg.Tentativas = 2

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := ConectarComConfig(ctx, cfg); err == nil {
		t.Fatal("conexão deveria falhar sem resposta dos dois brokers")
	}
}

func TestPedidoRespeitaExpiracaoDoContexto(t *testing.T) {
	b := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoAtividade {
			respondToClient(b, m, addr)
		}
	})
	cfg := configForBrokers(b)
	cfg.Repeticao = 300 * time.Millisecond
	c := connectTestClient(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := c.CriarTopico(ctx, "expiracao")
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrResultadoDesconhecido) {
		t.Fatalf("erro = %v; esperado contexto expirado com resultado desconhecido", err)
	}
}

func TestConectarRetornaExpiracaoDoContexto(t *testing.T) {
	b := newUDPTestBroker(t, nil)
	cfg := configForBrokers(b)
	cfg.Repeticao = 300 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := ConectarComConfig(ctx, cfg)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("erro = %v; esperado context.DeadlineExceeded", err)
	}
}

func TestConfiguracaoLimitaRepeticoesAoCacheDoBroker(t *testing.T) {
	_, err := ConectarComConfig(context.Background(), Config{
		Brokers:    []string{"127.0.0.1:8080"},
		Intervalo:  10 * time.Millisecond,
		Timeout:    20 * time.Millisecond,
		Repeticao:  2 * time.Second,
		Tentativas: 10,
	})
	if err == nil || !strings.Contains(err.Error(), "validade do cache") {
		t.Fatalf("configuração deveria exceder o cache do broker: %v", err)
	}
}

func TestFecharMarcaClienteDesconectado(t *testing.T) {
	b := newUDPTestBroker(t, respondToClient)
	c := connectTestClient(t, configForBrokers(b))
	if err := c.Fechar(); err != nil {
		t.Fatal(err)
	}
	if got := c.BrokerAtual(); got != "" {
		t.Fatalf("broker atual após Fechar = %q; esperado vazio", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Publicar(ctx, "fechado", []byte("não enviar")); !errors.Is(err, ErrDesconectado) {
		t.Fatalf("Publicar após Fechar retornou %v", err)
	}
	if _, err := c.Estatisticas(ctx); !errors.Is(err, ErrDesconectado) {
		t.Fatalf("Estatisticas após Fechar retornou %v", err)
	}
}

func TestFilaDeEventosDescartaMensagensMaisAntigasQuandoCheia(t *testing.T) {
	clientAddr := make(chan *net.UDPAddr, 1)
	b := newUDPTestBroker(t, func(b *udpTestBroker, m protocol.Mensagem, addr *net.UDPAddr) {
		if m.Tipo == protocol.TipoAssinar {
			clientAddr <- addr
		}
		respondToClient(b, m, addr)
	})
	cfg := configForBrokers(b)
	cfg.Buffer = 2
	c := connectTestClient(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Assinar(ctx, "fila"); err != nil {
		t.Fatal(err)
	}
	receiveEvent(t, c, protocol.TipoOK)
	addr := <-clientAddr
	sendMessage := func(id uint64) {
		t.Helper()
		err := b.send(addr, protocol.Mensagem{
			Tipo:    protocol.TipoMensagem,
			ID:      id,
			Cliente: "publicador",
			Topico:  "fila",
		})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return c.recebidas.Load() == id })
	}

	sendMessage(1)
	waitFor(t, func() bool { return len(c.dados) == 0 })
	time.Sleep(10 * time.Millisecond)
	for id := uint64(2); id <= 5; id++ {
		sendMessage(id)
	}
	if got := c.Descartadas(); got != 2 {
		t.Fatalf("mensagens descartadas = %d; esperado 2", got)
	}

	var delivered []uint64
	for i := 0; i < 3; i++ {
		m := receiveEvent(t, c, protocol.TipoMensagem)
		delivered = append(delivered, m.ID)
	}
	if want := []uint64{1, 4, 5}; !reflect.DeepEqual(delivered, want) {
		t.Fatalf("mensagens entregues = %v; esperado %v", delivered, want)
	}
}
