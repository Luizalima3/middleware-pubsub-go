package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/Luizalima3/middleware-pubsub-go/internal/avaliacao"
	"github.com/Luizalima3/middleware-pubsub-go/internal/client"
	"github.com/Luizalima3/middleware-pubsub-go/internal/protocol"
)

func iniciarBrokerTeste(t *testing.T, cfg Config) (*Broker, string, string) {
	t.Helper()
	cfg.Silencioso = true
	b := Novo(cfg)
	srv := httptest.NewServer(b.Handler())
	t.Cleanup(func() {
		_ = b.Fechar()
		srv.Close()
	})
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	return b, wsURL, srv.URL
}

func conectarCliente(t *testing.T, wsURL string) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Conectar(ctx, []string{wsURL})
	if err != nil {
		t.Fatalf("conectar cliente: %v", err)
	}
	t.Cleanup(func() { _ = c.Fechar() })
	return c
}

func lerAte(t *testing.T, c *client.Client, prazo time.Duration, pred func(protocol.Mensagem) bool) protocol.Mensagem {
	t.Helper()
	timer := time.NewTimer(prazo)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			t.Fatal("tempo esgotado aguardando mensagem esperada")
		case m, ok := <-c.Eventos():
			if !ok {
				t.Fatal("canal Eventos fechado inesperadamente")
			}
			if pred(m) {
				return m
			}
		}
	}
}

func esperarOK(t *testing.T, c *client.Client, id uint32) {
	t.Helper()
	m := lerAte(t, c, 3*time.Second, func(m protocol.Mensagem) bool {
		return m.ID == id && (m.Tipo == protocol.TipoOK || m.Tipo == protocol.TipoErro)
	})
	if m.Tipo != protocol.TipoOK {
		t.Fatalf("esperava TipoOK para id=%d, obteve tipo=%d conteudo=%q", id, m.Tipo, string(m.Conteudo))
	}
}

func esperarErro(t *testing.T, c *client.Client, id uint32, codigo string) {
	t.Helper()
	m := lerAte(t, c, 3*time.Second, func(m protocol.Mensagem) bool {
		return m.ID == id && m.Tipo == protocol.TipoErro
	})
	if string(m.Conteudo) != codigo {
		t.Fatalf("esperava erro %q, obteve %q", codigo, string(m.Conteudo))
	}
}

// 1. CriarTopico responde OK com o mesmo ID, e de novo OK na segunda vez (idempotente),
// e rejeita nome vazio com TOPICO_INVALIDO.
func TestCriarTopicoIdempotenteEInvalido(t *testing.T) {
	b, wsURL, _ := iniciarBrokerTeste(t, Config{})
	c := conectarCliente(t, wsURL)
	ctx := context.Background()

	id1, err := c.CriarTopico(ctx, "leilao/lances")
	if err != nil {
		t.Fatal(err)
	}
	esperarOK(t, c, id1)

	id2, err := c.CriarTopico(ctx, "leilao/lances")
	if err != nil {
		t.Fatal(err)
	}
	esperarOK(t, c, id2)

	idVazio, err := c.CriarTopico(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	esperarErro(t, c, idVazio, CodigoTopicoInvalido)

	if b.Contadores().TopicosAtivos != 1 {
		t.Fatalf("esperava 1 tópico ativo, obteve %d", b.Contadores().TopicosAtivos)
	}
}

// 2. Assinar responde OK com o mesmo ID (idempotente). Com N inscritos, uma
// publicação chega a todos com tópico, payload e ID originais.
func TestAssinarEFanoutPreservaIDTopicoConteudo(t *testing.T) {
	_, wsURL, _ := iniciarBrokerTeste(t, Config{})
	ctx := context.Background()

	pub := conectarCliente(t, wsURL)
	idCriar, err := pub.CriarTopico(ctx, "evento/reacoes")
	if err != nil {
		t.Fatal(err)
	}
	esperarOK(t, pub, idCriar)

	const numInscritos = 5
	subs := make([]*client.Client, numInscritos)
	for i := 0; i < numInscritos; i++ {
		subs[i] = conectarCliente(t, wsURL)
		idAssinar, err := subs[i].Assinar(ctx, "evento/reacoes")
		if err != nil {
			t.Fatal(err)
		}
		esperarOK(t, subs[i], idAssinar)
	}
	// Assinar repetido no mesmo cliente deve continuar idempotente e sem duplicar entrega.
	idRep, err := subs[0].Assinar(ctx, "evento/reacoes")
	if err != nil {
		t.Fatal(err)
	}
	esperarOK(t, subs[0], idRep)

	payload := []byte("Lance: R$ 500")
	idPub, err := pub.Publicar(ctx, "evento/reacoes", payload)
	if err != nil {
		t.Fatal(err)
	}

	for i, s := range subs {
		m := lerAte(t, s, 3*time.Second, func(m protocol.Mensagem) bool {
			return m.Tipo == protocol.TipoMensagem
		})
		if m.ID != idPub || m.Topico != "evento/reacoes" || !bytes.Equal(m.Conteudo, payload) {
			t.Fatalf("inscrito %d recebeu %+v, esperava id=%d topico=evento/reacoes conteudo=%q", i, m, idPub, payload)
		}
	}
}

// 3. Tópico sem inscritos gera TipoSemInscritos com o mesmo ID.
func TestPublicarSemInscritos(t *testing.T) {
	b, wsURL, _ := iniciarBrokerTeste(t, Config{})
	ctx := context.Background()
	c := conectarCliente(t, wsURL)

	idCriar, err := c.CriarTopico(ctx, "sala/vazia")
	if err != nil {
		t.Fatal(err)
	}
	esperarOK(t, c, idCriar)

	idPub, err := c.Publicar(ctx, "sala/vazia", []byte("alguém aí?"))
	if err != nil {
		t.Fatal(err)
	}
	m := lerAte(t, c, 3*time.Second, func(m protocol.Mensagem) bool {
		return m.ID == idPub
	})
	if m.Tipo != protocol.TipoSemInscritos || m.Topico != "sala/vazia" {
		t.Fatalf("esperava TipoSemInscritos, obteve %+v", m)
	}
	if b.Contadores().PublicacoesSemInscritos != 1 {
		t.Fatalf("contador PublicacoesSemInscritos = %d", b.Contadores().PublicacoesSemInscritos)
	}
}

// 4. Publicar ou Assinar em tópico inexistente geram TOPICO_INEXISTENTE.
func TestTopicoInexistente(t *testing.T) {
	b, wsURL, _ := iniciarBrokerTeste(t, Config{})
	ctx := context.Background()
	c := conectarCliente(t, wsURL)

	idAss, err := c.Assinar(ctx, "nao/existe")
	if err != nil {
		t.Fatal(err)
	}
	esperarErro(t, c, idAss, CodigoTopicoInexistente)

	idPub, err := c.Publicar(ctx, "nao/existe", []byte("oi"))
	if err != nil {
		t.Fatal(err)
	}
	esperarErro(t, c, idPub, CodigoTopicoInexistente)

	if b.Contadores().PublicacoesTopicoInexistente != 1 {
		t.Fatalf("contador PublicacoesTopicoInexistente = %d", b.Contadores().PublicacoesTopicoInexistente)
	}
}

// 5. Depois de Cancelar, o inscrito não recebe mais. Quando o último cancela,
// a publicação seguinte gera SemInscritos.
func TestCancelarInscricao(t *testing.T) {
	_, wsURL, _ := iniciarBrokerTeste(t, Config{})
	ctx := context.Background()

	pub := conectarCliente(t, wsURL)
	sub1 := conectarCliente(t, wsURL)
	sub2 := conectarCliente(t, wsURL)

	id, _ := pub.CriarTopico(ctx, "jogo/placar")
	esperarOK(t, pub, id)

	id, _ = sub1.Assinar(ctx, "jogo/placar")
	esperarOK(t, sub1, id)
	id, _ = sub2.Assinar(ctx, "jogo/placar")
	esperarOK(t, sub2, id)

	// sub1 cancela; publicação deve ir só para sub2.
	id, _ = sub1.Cancelar(ctx, "jogo/placar")
	esperarOK(t, sub1, id)

	// Cancelar de novo (sem estar inscrito) continua respondendo OK.
	id, _ = sub1.Cancelar(ctx, "jogo/placar")
	esperarOK(t, sub1, id)

	idPub1, _ := pub.Publicar(ctx, "jogo/placar", []byte("1x0"))
	m2 := lerAte(t, sub2, 3*time.Second, func(m protocol.Mensagem) bool {
		return m.Tipo == protocol.TipoMensagem
	})
	if m2.ID != idPub1 {
		t.Fatalf("sub2 recebeu ID %d, esperava %d", m2.ID, idPub1)
	}
	select {
	case m := <-sub1.Eventos():
		t.Fatalf("sub1 não deveria receber após cancelar, recebeu %+v", m)
	case <-time.After(80 * time.Millisecond):
	}

	// Último inscrito cancela -> próxima publicação devolve TipoSemInscritos.
	id, _ = sub2.Cancelar(ctx, "jogo/placar")
	esperarOK(t, sub2, id)

	idPub2, _ := pub.Publicar(ctx, "jogo/placar", []byte("2x0"))
	aviso := lerAte(t, pub, 3*time.Second, func(m protocol.Mensagem) bool {
		return m.ID == idPub2
	})
	if aviso.Tipo != protocol.TipoSemInscritos {
		t.Fatalf("esperava TipoSemInscritos após último cancelar, obteve %+v", aviso)
	}
}

// 6. ApagarTopico: inscritos recebem TOPICO_APAGADO (ID 0), e a publicação
// seguinte gera TOPICO_INEXISTENTE. Apagar tópico inexistente é idempotente.
func TestApagarTopico(t *testing.T) {
	_, wsURL, _ := iniciarBrokerTeste(t, Config{})
	ctx := context.Background()

	pub := conectarCliente(t, wsURL)
	sub := conectarCliente(t, wsURL)

	id, _ := pub.CriarTopico(ctx, "temp/sala")
	esperarOK(t, pub, id)
	id, _ = sub.Assinar(ctx, "temp/sala")
	esperarOK(t, sub, id)

	idApagar, _ := pub.ApagarTopico(ctx, "temp/sala")
	esperarOK(t, pub, idApagar)

	// Inscrito recebe aviso espontâneo (ID 0, TipoErro, TOPICO_APAGADO).
	aviso := lerAte(t, sub, 3*time.Second, func(m protocol.Mensagem) bool {
		return m.Tipo == protocol.TipoErro
	})
	if aviso.ID != 0 || aviso.Topico != "temp/sala" || string(aviso.Conteudo) != CodigoTopicoApagado {
		t.Fatalf("aviso inesperado ao apagar tópico: %+v (%q)", aviso, string(aviso.Conteudo))
	}

	// Publicação seguinte gera TOPICO_INEXISTENTE.
	idPub, _ := pub.Publicar(ctx, "temp/sala", []byte("depois"))
	esperarErro(t, pub, idPub, CodigoTopicoInexistente)

	// Apagar novamente é idempotente.
	idApagar2, _ := pub.ApagarTopico(ctx, "temp/sala")
	esperarOK(t, pub, idApagar2)
}

// 7. Rajada de 1000 publicações seguidas (limite de taxa alto no teste) com
// consumidores rápidos: todas entregues.
func TestRajada1000PublicacoesConsumidoresRapidos(t *testing.T) {
	_, wsURL, _ := iniciarBrokerTeste(t, Config{
		Taxa:          100000,
		Rajada:        10000,
		FilaAssinante: 2048,
		FilaTopico:    4096,
	})
	ctx := context.Background()

	pub := conectarCliente(t, wsURL)
	id, _ := pub.CriarTopico(ctx, "rajada/1000")
	esperarOK(t, pub, id)

	const (
		total   = 1000
		numSubs = 3
	)
	subs := make([]*client.Client, numSubs)
	for i := 0; i < numSubs; i++ {
		subs[i] = conectarCliente(t, wsURL)
		idAss, _ := subs[i].Assinar(ctx, "rajada/1000")
		esperarOK(t, subs[i], idAss)
	}

	var wg sync.WaitGroup
	errosSub := make(chan error, numSubs)
	for i, s := range subs {
		wg.Add(1)
		go func(idx int, c *client.Client) {
			defer wg.Done()
			prazo := time.After(10 * time.Second)
			for recebidas := 0; recebidas < total; {
				select {
				case <-prazo:
					errosSub <- fmt.Errorf("assinante %d recebeu apenas %d/%d", idx, recebidas, total)
					return
				case m := <-c.Eventos():
					if m.Tipo == protocol.TipoMensagem {
						recebidas++
					}
				}
			}
		}(i, s)
	}

	for i := 0; i < total; i++ {
		if _, err := pub.Publicar(ctx, "rajada/1000", []byte(fmt.Sprintf("msg-%04d", i))); err != nil {
			t.Fatalf("publicar %d: %v", i, err)
		}
	}

	wg.Wait()
	close(errosSub)
	for err := range errosSub {
		t.Fatal(err)
	}
}

// discarComBufferCurto abre uma conexão WebSocket cujo socket TCP subjacente
// tem buffers de recepção pequenos (4 KiB), para que um consumidor que para de
// ler exercite imediatamente a contrapressão TCP em loopback no Windows.
func discarComBufferCurto(ctx context.Context, wsURL string) (*websocket.Conn, error) {
	hc := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				var d net.Dialer
				c, err := d.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.SetReadBuffer(4096)
				}
				return c, nil
			},
		},
	}
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPClient: hc})
	return conn, err
}

// 8. Consumidor lento isolado: um inscrito rápido e um que para de ler.
// O rápido recebe todas com latência baixa (p99 registrado no log), o broker
// não bloqueia e entregas_descartadas_consumidor_lento > 0. Quando o lento
// volta a ler dentro do timeout, recebe TipoDescartadas com contagem > 0.
func TestConsumidorLentoIsoladoEDescartadas(t *testing.T) {
	b, wsURL, _ := iniciarBrokerTeste(t, Config{
		FilaAssinante:  16,
		FilaTopico:     1024,
		Taxa:           100000,
		Rajada:         10000,
		TimeoutEscrita: 5 * time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub := conectarCliente(t, wsURL)
	rapido := conectarCliente(t, wsURL)

	id, _ := pub.CriarTopico(ctx, "evento/lento")
	esperarOK(t, pub, id)
	id, _ = rapido.Assinar(ctx, "evento/lento")
	esperarOK(t, rapido, id)

	// Consumidor lento com conexão coder/websocket crua que para de ler.
	connLento, err := discarComBufferCurto(ctx, wsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer connLento.CloseNow()
	connLento.SetReadLimit(protocol.MaxMensagem)

	quadroAssinar, _ := protocol.Codificar(protocol.Mensagem{
		Tipo:   protocol.TipoAssinar,
		ID:     77,
		Topico: "evento/lento",
	})
	if err := connLento.Write(ctx, websocket.MessageBinary, quadroAssinar); err != nil {
		t.Fatal(err)
	}
	_, dadosOK, err := connLento.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	msgOK, err := protocol.Decodificar(dadosOK)
	if err != nil || msgOK.Tipo != protocol.TipoOK || msgOK.ID != 77 {
		t.Fatalf("esperava TipoOK id=77 no lento, obteve %+v err=%v", msgOK, err)
	}

	// Agora connLento NÃO lê. Enviamos 160 mensagens de 32 KiB com 2 ms de
	// intervalo reutilizando o buffer: o rápido consome todas imediatamente;
	// connLento enche o buffer TCP (~4 MB) e estoura sua fila de 16 posições.
	const total = 160
	payload := bytes.Repeat([]byte("Z"), 32*1024)
	latencias := make([]time.Duration, 0, total)
	prontoRapido := make(chan struct{})
	go func() {
		defer close(prontoRapido)
		for len(latencias) < total {
			m, ok := <-rapido.Eventos()
			if !ok {
				return
			}
			if m.Tipo == protocol.TipoMensagem && len(m.Conteudo) >= 8 {
				tEnvio := int64(binary.BigEndian.Uint64(m.Conteudo[:8]))
				latencias = append(latencias, time.Duration(time.Now().UnixNano()-tEnvio))
			}
		}
	}()

	for i := 0; i < total; i++ {
		binary.BigEndian.PutUint64(payload[:8], uint64(time.Now().UnixNano()))
		if _, err := pub.Publicar(ctx, "evento/lento", payload); err != nil {
			t.Fatalf("publicar %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	select {
	case <-prontoRapido:
	case <-time.After(3 * time.Second):
		t.Fatalf("consumidor rápido recebeu apenas %d/%d", len(latencias), total)
	}

	sort.Slice(latencias, func(i, j int) bool { return latencias[i] < latencias[j] })
	p50 := latencias[len(latencias)*50/100]
	p99 := latencias[len(latencias)*99/100]
	t.Logf("consumidor rápido recebeu %d/%d mensagens: p50=%v p99=%v", len(latencias), total, p50, p99)
	if p99 > 250*time.Millisecond {
		t.Fatalf("p99 do consumidor rápido muito alta (%v) na presença de lento", p99)
	}

	cont := b.Contadores()
	if cont.EntregasDescartadasConsumidorLento == 0 {
		t.Fatalf("esperava entregas_descartadas_consumidor_lento > 0, obteve 0")
	}
	if cont.Topicos["evento/lento"].Descartes == 0 {
		t.Fatalf("esperava descartes > 0 no tópico evento/lento")
	}

	// Quando o consumidor lento volta a ler (dentro do timeout de escrita),
	// deve receber TipoDescartadas com contagem > 0.
	var descartadasReportadas uint32
	prazoLeitura, cancelLeitura := context.WithTimeout(ctx, 2*time.Second)
	defer cancelLeitura()
	for descartadasReportadas == 0 {
		_, dados, err := connLento.Read(prazoLeitura)
		if err != nil {
			t.Fatalf("lento falhou ao voltar a ler: %v", err)
		}
		m, err := protocol.Decodificar(dados)
		if err != nil {
			t.Fatal(err)
		}
		if m.Tipo == protocol.TipoDescartadas {
			if m.ID != 0 || m.Topico != "evento/lento" || len(m.Conteudo) != 4 {
				t.Fatalf("frame TipoDescartadas malformado: %+v", m)
			}
			descartadasReportadas = binary.BigEndian.Uint32(m.Conteudo)
		}
	}
	t.Logf("consumidor lento recebeu TipoDescartadas=%d (descartes totais no broker=%d)",
		descartadasReportadas, cont.EntregasDescartadasConsumidorLento)
}

// 9. Timeout de escrita: com um inscrito travado e timeout curto, a sessão é
// encerrada e removida do tópico.
func TestTimeoutEscritaDesconectaClienteTravado(t *testing.T) {
	b, wsURL, _ := iniciarBrokerTeste(t, Config{
		FilaAssinante:  64,
		TimeoutEscrita: 60 * time.Millisecond,
		Taxa:           100000,
		Rajada:         10000,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pub := conectarCliente(t, wsURL)
	id, _ := pub.CriarTopico(ctx, "evento/travado")
	esperarOK(t, pub, id)

	connTravada, err := discarComBufferCurto(ctx, wsURL)
	if err != nil {
		t.Fatal(err)
	}
	defer connTravada.CloseNow()

	quadroAssinar, _ := protocol.Codificar(protocol.Mensagem{
		Tipo:   protocol.TipoAssinar,
		ID:     1,
		Topico: "evento/travado",
	})
	if err := connTravada.Write(ctx, websocket.MessageBinary, quadroAssinar); err != nil {
		t.Fatal(err)
	}
	_, _, err = connTravada.Read(ctx) // consome o TipoOK
	if err != nil {
		t.Fatal(err)
	}

	// Enche os buffers TCP de envio/recepção enviando payloads grandes de 60 KiB
	// continuamente sem ler em connTravada até o escritor estourar o TimeoutEscrita.
	bloco := bytes.Repeat([]byte("X"), 60*1024)
	prazo := time.Now().Add(4 * time.Second)
	for time.Now().Before(prazo) {
		for i := 0; i < 128; i++ {
			_, _ = pub.Publicar(ctx, "evento/travado", bloco)
			if b.Contadores().SessoesEncerradasTimeoutEscrita >= 1 {
				break
			}
		}
		c := b.Contadores()
		if c.SessoesEncerradasTimeoutEscrita >= 1 && c.Topicos["evento/travado"].Inscritos == 0 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	c := b.Contadores()
	t.Logf("após rajada em cliente travado: contadores=%+v topico=%+v", c, c.Topicos["evento/travado"])
	if c.SessoesEncerradasTimeoutEscrita < 1 {
		t.Fatalf("esperava sessoes_encerradas_timeout_escrita >= 1, obteve %d", c.SessoesEncerradasTimeoutEscrita)
	}
	if c.Topicos["evento/travado"].Inscritos != 0 {
		t.Fatalf("sessão travada deveria ter sido removida do tópico, inscritos=%d", c.Topicos["evento/travado"].Inscritos)
	}
}

// 10. Ponto de integração do rate limit: com bucket pequeno, a resposta é
// RATE_LIMIT com o ID original, e a mensagem recusada não é entregue.
func TestIntegracaoRateLimit(t *testing.T) {
	b, wsURL, _ := iniciarBrokerTeste(t, Config{
		Taxa:   0.001, // praticamente sem reposição durante o teste
		Rajada: 2,
	})
	ctx := context.Background()

	pub := conectarCliente(t, wsURL)
	sub := conectarCliente(t, wsURL)

	id, _ := pub.CriarTopico(ctx, "evento/cota")
	esperarOK(t, pub, id)
	id, _ = sub.Assinar(ctx, "evento/cota")
	esperarOK(t, sub, id)

	// 2 primeiras passam; a 3ª excede a rajada e deve receber RATE_LIMIT.
	id1, _ := pub.Publicar(ctx, "evento/cota", []byte("m1"))
	id2, _ := pub.Publicar(ctx, "evento/cota", []byte("m2"))
	id3, _ := pub.Publicar(ctx, "evento/cota", []byte("m3-bloqueada"))

	esperarErro(t, pub, id3, "RATE_LIMIT")

	m1 := lerAte(t, sub, 3*time.Second, func(m protocol.Mensagem) bool { return m.Tipo == protocol.TipoMensagem })
	m2 := lerAte(t, sub, 3*time.Second, func(m protocol.Mensagem) bool { return m.Tipo == protocol.TipoMensagem })
	if m1.ID != id1 || m2.ID != id2 {
		t.Fatalf("IDs entregues inesperados: %d, %d (esperava %d, %d)", m1.ID, m2.ID, id1, id2)
	}

	select {
	case extra := <-sub.Eventos():
		t.Fatalf("mensagem recusada pelo rate limit não deveria ser entregue: %+v", extra)
	case <-time.After(80 * time.Millisecond):
	}

	if b.Contadores().PublicacoesRejeitadasRateLimit != 1 {
		t.Fatalf("contador PublicacoesRejeitadasRateLimit = %d", b.Contadores().PublicacoesRejeitadasRateLimit)
	}
}

// 11. Frame inválido (ou de texto, ou tipo desconhecido) gera erro adequado e
// a conexão continua utilizável.
func TestFrameInvalidoETipoDesconhecidoMantemConexao(t *testing.T) {
	_, wsURL, httpURL := iniciarBrokerTeste(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()

	// 1) Frame de texto -> FRAME_INVALIDO (ID 0).
	if err := conn.Write(ctx, websocket.MessageText, []byte("texto puro")); err != nil {
		t.Fatal(err)
	}
	_, dados, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m, err := protocol.Decodificar(dados)
	if err != nil || m.Tipo != protocol.TipoErro || m.ID != 0 || string(m.Conteudo) != CodigoFrameInvalido {
		t.Fatalf("resposta para frame de texto: %+v err=%v", m, err)
	}

	// 2) Frame binário truncado -> FRAME_INVALIDO (ID 0).
	if err := conn.Write(ctx, websocket.MessageBinary, []byte{1, 2}); err != nil {
		t.Fatal(err)
	}
	_, dados, err = conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m, err = protocol.Decodificar(dados)
	if err != nil || m.Tipo != protocol.TipoErro || m.ID != 0 || string(m.Conteudo) != CodigoFrameInvalido {
		t.Fatalf("resposta para frame truncado: %+v err=%v", m, err)
	}

	// 3) Tipo reservado broker->cliente (TipoOK=17) enviado pelo cliente -> TIPO_DESCONHECIDO.
	quadroTipoInvalido, _ := protocol.Codificar(protocol.Mensagem{
		Tipo:   protocol.TipoOK,
		ID:     55,
		Topico: "x",
	})
	if err := conn.Write(ctx, websocket.MessageBinary, quadroTipoInvalido); err != nil {
		t.Fatal(err)
	}
	_, dados, err = conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m, err = protocol.Decodificar(dados)
	if err != nil || m.Tipo != protocol.TipoErro || m.ID != 55 || string(m.Conteudo) != CodigoTipoDesconhecido {
		t.Fatalf("resposta para tipo inválido: %+v err=%v", m, err)
	}

	// 4) A mesma conexão segue utilizável: CriarTopico funciona normalmente.
	quadroCriar, _ := protocol.Codificar(protocol.Mensagem{
		Tipo:   protocol.TipoCriarTopico,
		ID:     99,
		Topico: "conexao/viva",
	})
	if err := conn.Write(ctx, websocket.MessageBinary, quadroCriar); err != nil {
		t.Fatal(err)
	}
	_, dados, err = conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m, err = protocol.Decodificar(dados)
	if err != nil || m.Tipo != protocol.TipoOK || m.ID != 99 {
		t.Fatalf("conexão não continuou utilizável após frame inválido: %+v err=%v", m, err)
	}

	// Verifica também o endpoint HTTP GET /contadores.
	resp, err := http.Get(httpURL + "/contadores")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /contadores status=%d", resp.StatusCode)
	}
	var snap Contadores
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decodificar /contadores: %v", err)
	}
	if snap.TopicosAtivos != 1 || snap.ConexoesAtivas < 1 {
		t.Fatalf("snapshot inesperado em /contadores: %+v", snap)
	}
}

// 12. Sem vazamento de goroutines após desconexões.
func TestSemVazamentoDeGoroutines(t *testing.T) {
	b, wsURL, _ := iniciarBrokerTeste(t, Config{})
	ctx := context.Background()

	// Estabiliza goroutines de base.
	runtime.GC()
	time.Sleep(30 * time.Millisecond)
	antes := runtime.NumGoroutine()

	for rodada := 0; rodada < 20; rodada++ {
		c, err := client.Conectar(ctx, []string{wsURL})
		if err != nil {
			t.Fatal(err)
		}
		nomeTopico := fmt.Sprintf("efemero/%d", rodada)
		id, _ := c.CriarTopico(ctx, nomeTopico)
		esperarOK(t, c, id)
		id, _ = c.Assinar(ctx, nomeTopico)
		esperarOK(t, c, id)
		_, _ = c.Publicar(ctx, nomeTopico, []byte("ping"))
		lerAte(t, c, 2*time.Second, func(m protocol.Mensagem) bool { return m.Tipo == protocol.TipoMensagem })
		id, _ = c.ApagarTopico(ctx, nomeTopico)
		esperarOK(t, c, id)
		_ = c.Fechar()
	}

	prazo := time.Now().Add(2 * time.Second)
	for time.Now().Before(prazo) {
		if b.Contadores().ConexoesAtivas == 0 && b.Contadores().TopicosAtivos == 0 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	depois := runtime.NumGoroutine()
	t.Logf("goroutines antes=%d depois=%d conexoes_ativas=%d topicos_ativos=%d",
		antes, depois, b.Contadores().ConexoesAtivas, b.Contadores().TopicosAtivos)
	if b.Contadores().ConexoesAtivas != 0 || b.Contadores().TopicosAtivos != 0 {
		t.Fatalf("recursos não limpos: %+v", b.Contadores())
	}
	if depois > antes+4 {
		t.Fatalf("possível vazamento de goroutines: antes=%d depois=%d", antes, depois)
	}
}

// 13. Compatibilidade com a Parte 3: avaliacao.Executar com configuração
// pequena contra o broker real termina sem erro, com Ausentes == 0 em carga leve.
func TestCompatibilidadeGeradorDeCargaParte3(t *testing.T) {
	_, wsURL, _ := iniciarBrokerTeste(t, Config{
		Taxa:   1000,
		Rajada: 1000,
	})
	cfg := avaliacao.Config{
		URLs:         []string{wsURL},
		Topico:       "avaliacao/integracao",
		Publicadores: 2,
		Assinantes:   4,
		Taxa:         20,
		Duracao:      300 * time.Millisecond,
		Drenagem:     200 * time.Millisecond,
		Timeout:      3 * time.Second,
		Payload:      64,
		Fila:         32,
	}
	res, err := avaliacao.Executar(context.Background(), cfg)
	if err != nil {
		t.Fatalf("avaliacao.Executar falhou contra o broker real: %v", err)
	}
	s := res.Resumo
	t.Logf("avaliacao.Executar: enviadas=%d recebidas=%d ausentes=%d p50=%.3fms p99=%.3fms",
		s.Enviadas, s.Recebidas, s.Ausentes, s.P50MS, s.P99MS)
	if s.Enviadas == 0 || s.Ausentes != 0 || s.ErrosEnvio != 0 || s.OutrosErros != 0 {
		t.Fatalf("resumo inesperado na integração com Parte 3: %+v", s)
	}
}
