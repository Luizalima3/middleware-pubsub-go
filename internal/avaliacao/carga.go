// Package avaliacao executa carga em agenda fixa e coleta métricas no processo
// do gerador. Não substitui as métricas de CPU e memória do broker.
package avaliacao

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/client"
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
)

type Config struct {
	URLs              []string      `json:"brokers"`
	Topico            string        `json:"topico"`
	Publicadores      int           `json:"publicadores"`
	Assinantes        int           `json:"assinantes"`
	Taxa              float64       `json:"taxa_por_publicador"`
	Duracao           time.Duration `json:"duracao_ns"`
	Drenagem          time.Duration `json:"drenagem_ns"`
	Timeout           time.Duration `json:"timeout_ns"`
	Payload           int           `json:"payload_bytes"`
	Fila              int           `json:"fila_gerador"`
	Lentos            int           `json:"assinantes_lentos"`
	AtrasoLento       time.Duration `json:"atraso_consumo_ns"`
	IgnorarControle   bool          `json:"ignorar_controle"`
	Distribuir        bool          `json:"distribuir_brokers"`
	BufferCliente     int           `json:"buffer_cliente"`
	TopicoExistente   bool          `json:"usar_topico_existente"`
	IntervaloMetricas time.Duration `json:"intervalo_metricas_ns"`
}

func (c Config) validar() (int, error) {
	if len(c.URLs) == 0 || c.Topico == "" || len(c.Topico) > protocol.MaxTopico {
		return 0, errors.New("brokers e tópico válido são obrigatórios")
	}
	if c.Publicadores < 1 || c.Assinantes < 1 || c.Lentos < 0 || c.Lentos > c.Assinantes {
		return 0, errors.New("quantidades de clientes inválidas")
	}
	if c.Taxa <= 0 || math.IsNaN(c.Taxa) || math.IsInf(c.Taxa, 0) || c.Duracao <= 0 || c.Duracao > 10*time.Minute || c.Drenagem < 0 || c.Drenagem > time.Minute || c.Timeout <= 0 || c.Fila < 1 || c.AtrasoLento < 0 {
		return 0, errors.New("taxa, duração, timeout e fila devem ser positivos; atrasos não negativos")
	}
	if c.Payload < 96 || c.Payload > 512 {
		return 0, errors.New("payload deve ter entre 96 e 512 bytes para caber no JSON de 1000 bytes")
	}
	n := math.Ceil(c.Taxa * c.Duracao.Seconds())
	if n < 1 || n > 1e6 || float64(c.Publicadores)*n > 1e6 || float64(c.Publicadores)*n*float64(c.Assinantes) > 1e8 || c.Publicadores+c.Assinantes > 10000 || c.Fila > 10000 {
		return 0, errors.New("carga excede os limites do gerador: 1 milhão de publicações, 100 milhões de entregas, 10 mil clientes/fila")
	}
	return int(n), nil
}

type Linha struct {
	TaxaMedia                                                                      float64 `json:"taxa_media_cliente"`
	AmostrasTaxa                                                                   uint64  `json:"amostras_taxa"`
	Segundo                                                                        int     `json:"segundo"`
	Oferecidas, Enviadas, ErrosEnvio, Puladas, Recebidas, Rejeitadas, SemInscritos uint64
	P99MS                                                                          float64 `json:"p99_ms"`
	h                                                                              histograma
}

type Resumo struct {
	Config             Config    `json:"configuracao"`
	Inicio             time.Time `json:"inicio"`
	JanelaSegundos     float64   `json:"janela_observada_segundos"`
	Planejadas         uint64    `json:"publicacoes_planejadas"`
	NaoAgendadas       uint64    `json:"publicacoes_nao_agendadas"`
	Oferecidas         uint64    `json:"publicacoes_oferecidas"`
	Enviadas           uint64    `json:"publicacoes_escritas_no_socket"`
	Puladas            uint64    `json:"publicacoes_puladas_pelo_gerador"`
	ErrosEnvio         uint64    `json:"erros_de_envio"`
	Recebidas          uint64    `json:"entregas_unicas"`
	Duplicadas         uint64    `json:"entregas_duplicadas"`
	DuplicadasCliente  uint64    `json:"duplicatas_descartadas_pelo_cliente"`
	DuplicadasGerador  uint64    `json:"duplicatas_detectadas_pelo_gerador"`
	Ausentes           uint64    `json:"entregas_ausentes_entre_envios_sem_erro"`
	Rejeitadas         uint64    `json:"avisos_rate_limit"`
	SemInscritos       uint64    `json:"avisos_sem_inscritos"`
	OutrosErros        uint64    `json:"avisos_outros_erros"`
	DescartesBroker    uint64    `json:"avisos_descartes_broker"`
	DescartesCliente   uint64    `json:"descartes_fila_cliente"`
	TaxaFinalMedia     float64   `json:"taxa_final_media"`
	Invalidas          uint64    `json:"mensagens_de_teste_invalidas"`
	P50MS              float64   `json:"p50_ms"`
	P95MS              float64   `json:"p95_ms"`
	P99MS              float64   `json:"p99_ms"`
	P99NormaisMS       float64   `json:"p99_assinantes_normais_ms"`
	P99LentosMS        float64   `json:"p99_assinantes_lentos_ms"`
	P99AtrasoGeradorMS float64   `json:"p99_atraso_gerador_ms"`
	VazaoPublicacoes   float64   `json:"publicacoes_escritas_por_segundo_configurado"`
	VazaoEntregas      float64   `json:"entregas_por_segundo_observado"`
	Interrompido       bool      `json:"interrompido"`
}

type Resultado struct {
	Resumo Resumo
	Linhas []Linha
	Filas  []LinhaFila
}

type LinhaFila struct {
	Instante      time.Time
	Segundos      float64
	Papel         string
	Indice        int
	Fila          string
	Itens, Limite int
}
type coletor struct {
	sync.Mutex
	r                         Resumo
	linhas                    []Linha
	lat, normais, lentos, lag histograma
	seen                      [][]uint64
	sent                      []bool
	inicio                    time.Time
	run                       uint64
	n                         int
	filasEnvio                []chan int
	filas                     []LinhaFila
}

func (m *coletor) linha(t time.Time) *Linha {
	i := int(t.Sub(m.inicio).Seconds())
	if i < 0 {
		i = 0
	}
	if i >= len(m.linhas) {
		i = len(m.linhas) - 1
	}
	return &m.linhas[i]
}

func esperar(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Executar prepara clientes UDP e mede do horário programado ao consumo.
func Executar(ctx context.Context, cfg Config) (Resultado, error) {
	if cfg.IntervaloMetricas == 0 {
		cfg.IntervaloMetricas = 200 * time.Millisecond
	}
	if cfg.IntervaloMetricas < 10*time.Millisecond || cfg.IntervaloMetricas > 10*time.Second {
		return Resultado{}, errors.New("intervalo de métricas deve estar entre 10ms e 10s")
	}
	linhasMetricas := math.Ceil(float64(cfg.Duracao+cfg.Drenagem)/float64(cfg.IntervaloMetricas)) + 2
	if linhasMetricas*float64(2*(cfg.Publicadores+cfg.Assinantes+1)+cfg.Publicadores) > 1e6 {
		return Resultado{}, errors.New("mais de um milhão de amostras de filas; aumente o intervalo de métricas")
	}
	n, err := cfg.validar()
	if err != nil {
		return Resultado{}, err
	}
	var random [8]byte
	if _, err = rand.Read(random[:]); err != nil {
		return Resultado{}, err
	}
	var clientes []*client.Client
	defer func() {
		var wg sync.WaitGroup
		for _, c := range clientes {
			wg.Add(1)
			go func(c *client.Client) { defer wg.Done(); _ = c.Fechar() }(c)
		}
		wg.Wait()
	}()
	indiceConexao := 0
	abrir := func() (*client.Client, error) {
		cctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
		urls := append([]string(nil), cfg.URLs...)
		if cfg.Distribuir && len(urls) > 1 {
			j := indiceConexao % len(urls)
			urls = append(urls[j:], urls[:j]...)
		}
		indiceConexao++
		c, e := client.ConectarComConfig(cctx, client.Config{Brokers: urls, TaxaMaxima: cfg.Taxa, Buffer: cfg.BufferCliente, IgnorarControle: cfg.IgnorarControle})
		if e == nil {
			clientes = append(clientes, c)
		}
		return c, e
	}
	admin, err := abrir()
	if err != nil {
		return Resultado{}, err
	}
	if !cfg.TopicoExistente {
		prep, cancel := context.WithTimeout(ctx, cfg.Timeout)
		_, e := admin.CriarTopico(prep, cfg.Topico)

		cancel()
		if e != nil {
			return Resultado{}, fmt.Errorf("criação do tópico (exige TipoOK): %w", e)
		}
	}
	subs := make([]*client.Client, cfg.Assinantes)
	for i := range subs {
		subs[i], err = abrir()
		if err != nil {
			return Resultado{}, fmt.Errorf("assinante %d: %w", i, err)
		}
		prep, cancel := context.WithTimeout(ctx, cfg.Timeout)
		_, err := subs[i].Assinar(prep, cfg.Topico)

		cancel()
		if err != nil {
			return Resultado{}, fmt.Errorf("inscrição %d (exige TipoOK): %w", i, err)
		}
	}
	pubs := make([]*client.Client, cfg.Publicadores)
	for i := range pubs {
		pubs[i], err = abrir()
		if err != nil {
			return Resultado{}, err
		}
	}
	inicio := time.Now().Add(100 * time.Millisecond)
	m := &coletor{r: Resumo{Config: cfg, Inicio: inicio, Planejadas: uint64(n * cfg.Publicadores)}, inicio: inicio, run: binary.BigEndian.Uint64(random[:]), n: n, sent: make([]bool, n*cfg.Publicadores), seen: make([][]uint64, cfg.Assinantes), linhas: make([]Linha, int(math.Ceil((cfg.Duracao+cfg.Drenagem).Seconds()))+2)}
	for i := range m.seen {
		m.seen[i] = make([]uint64, (n*cfg.Publicadores+63)/64)
	}
	for i := range m.linhas {
		m.linhas[i].Segundo = i
	}
	m.filasEnvio = make([]chan int, len(pubs))
	readCtx, stopReaders := context.WithCancel(ctx)
	defer stopReaders()
	var readers sync.WaitGroup
	readers.Add(1)
	go func() { defer readers.Done(); m.amostrarFilas(readCtx, admin, subs, pubs) }()
	for i, c := range subs {
		readers.Add(1)
		go func(i int, c *client.Client) { defer readers.Done(); m.ler(readCtx, c, i) }(i, c)
	}
	for _, c := range append(pubs, admin) {
		readers.Add(1)
		go func(c *client.Client) { defer readers.Done(); m.avisos(readCtx, c) }(c)
	}
	sendCtx, stopSend := context.WithDeadline(ctx, inicio.Add(cfg.Duracao))
	defer stopSend()
	var writers sync.WaitGroup
	for p, c := range pubs {
		writers.Add(1)
		go func(p int, c *client.Client) { defer writers.Done(); m.publicador(sendCtx, c, p) }(p, c)
	}
	writers.Wait()
	// A janela termina no instante configurado, mesmo se o último slot for anterior.
	esperar(ctx, time.Until(inicio.Add(cfg.Duracao+cfg.Drenagem)))
	stopReaders()
	readers.Wait()
	for _, c := range subs {
		// Congela os contadores da biblioteca após encerrar a janela observada.
		_ = c.Fechar()
		m.r.DescartesCliente += c.Descartadas()
		m.r.DuplicadasCliente += c.Duplicadas()
	}
	m.r.Duplicadas = m.r.DuplicadasCliente + m.r.DuplicadasGerador
	for _, c := range pubs {
		m.r.TaxaFinalMedia += c.Taxa()
	}
	m.r.TaxaFinalMedia /= float64(len(pubs))
	m.r.JanelaSegundos = math.Max(0, time.Since(inicio).Seconds())
	m.r.Interrompido = ctx.Err() != nil
	m.r.NaoAgendadas = m.r.Planejadas - m.r.Oferecidas
	m.r.P50MS = m.lat.percentil(.5)
	m.r.P95MS = m.lat.percentil(.95)
	m.r.P99MS = m.lat.percentil(.99)
	m.r.P99NormaisMS = m.normais.percentil(.99)
	m.r.P99LentosMS = m.lentos.percentil(.99)
	m.r.P99AtrasoGeradorMS = m.lag.percentil(.99)
	for index, sent := range m.sent {
		if sent {
			for _, seen := range m.seen {
				if seen[index/64]&(uint64(1)<<uint(index%64)) == 0 {
					m.r.Ausentes++
				}
			}
		}
	}
	m.r.VazaoPublicacoes = float64(m.r.Enviadas) / cfg.Duracao.Seconds()
	if m.r.JanelaSegundos > 0 {
		m.r.VazaoEntregas = float64(m.r.Recebidas) / m.r.JanelaSegundos
	}
	for i := range m.linhas {
		m.linhas[i].P99MS = m.linhas[i].h.percentil(.99)
		if m.linhas[i].AmostrasTaxa > 0 {
			m.linhas[i].TaxaMedia /= float64(m.linhas[i].AmostrasTaxa)
		}
	}
	return Resultado{Resumo: m.r, Linhas: m.linhas, Filas: m.filas}, nil
}

func (m *coletor) publicador(ctx context.Context, c *client.Client, p int) {
	jobs := make(chan int, m.r.Config.Fila)
	m.Lock()
	m.filasEnvio[p] = jobs
	m.Unlock()
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		for seq := range jobs {
			if ctx.Err() != nil {
				m.Lock()
				m.r.Puladas++
				m.linha(time.Now()).Puladas++
				m.Unlock()
				continue
			}
			alvo := m.inicio.Add(time.Duration(float64(seq) / m.r.Config.Taxa * float64(time.Second)))
			dados := bytes.Repeat([]byte("."), m.r.Config.Payload)
			copy(dados, fmt.Sprintf("PSC1:%016x:%08x:%016x:%016x:", m.run, p, seq, uint64(alvo.Sub(m.inicio))))
			m.Lock()
			m.lag.registrar(time.Since(alvo))
			m.Unlock()
			writeCtx, cancel := context.WithTimeout(ctx, m.r.Config.Timeout)
			_, err := c.Publicar(writeCtx, m.r.Config.Topico, dados)
			cancel()
			m.Lock()
			if err != nil {
				m.r.ErrosEnvio++
				m.linha(time.Now()).ErrosEnvio++
			} else {
				m.r.Enviadas++
				m.sent[p*m.n+seq] = true
				m.linha(time.Now()).Enviadas++
			}
			m.Unlock()
		}
	}()
	defer worker.Wait()
	defer close(jobs)
	for seq := 0; seq < m.n; seq++ {
		alvo := m.inicio.Add(time.Duration(float64(seq) / m.r.Config.Taxa * float64(time.Second)))
		if !esperar(ctx, time.Until(alvo)) {
			return
		}
		m.Lock()
		m.r.Oferecidas++
		l := m.linha(alvo)
		l.TaxaMedia += c.Taxa()
		l.AmostrasTaxa++
		m.linha(alvo).Oferecidas++
		m.Unlock()
		select {
		case jobs <- seq:
		default:
			m.Lock()
			m.r.Puladas++
			m.linha(alvo).Puladas++
			m.Unlock()
		}
	}
}

func (m *coletor) ler(ctx context.Context, c *client.Client, sub int) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.Eventos():
			if !ok {
				return
			}
			if msg.Tipo != protocol.TipoMensagem {
				if msg.Tipo == protocol.TipoDescartadas {
					n, _ := strconv.ParseUint(string(msg.Conteudo), 10, 64)
					m.Lock()
					m.r.DescartesBroker += n
					m.Unlock()
				}
				continue
			}
			if sub < m.r.Config.Lentos && !esperar(ctx, m.r.Config.AtrasoLento) {
				return
			}
			m.receber(msg, sub, time.Now())
		}
	}
}

func (m *coletor) receber(msg protocol.Mensagem, sub int, agora time.Time) {
	d := msg.Conteudo
	parts := strings.SplitN(string(d), ":", 6)
	if len(parts) != 6 || parts[0] != "PSC1" {
		return
	}
	run, e0 := strconv.ParseUint(parts[1], 16, 64)
	p, e1 := strconv.ParseUint(parts[2], 16, 32)
	seq, e2 := strconv.ParseUint(parts[3], 16, 64)
	offset, e3 := strconv.ParseUint(parts[4], 16, 64)
	if e0 != nil || e1 != nil || e2 != nil || e3 != nil || run != m.run {
		return
	}

	m.Lock()
	defer m.Unlock()
	if p >= uint64(m.r.Config.Publicadores) || seq >= uint64(m.n) || offset > uint64(m.r.Config.Duracao) || msg.Topico != m.r.Config.Topico {
		m.r.Invalidas++
		return
	}
	lat := agora.Sub(m.inicio) - time.Duration(offset)
	if lat < 0 {
		m.r.Invalidas++
		return
	}
	index := int(p)*m.n + int(seq)
	bit := uint64(1) << uint(index%64)
	if m.seen[sub][index/64]&bit != 0 {
		m.r.DuplicadasGerador++
		return
	}
	m.seen[sub][index/64] |= bit
	m.r.Recebidas++
	m.lat.registrar(lat)
	l := m.linha(agora)
	l.Recebidas++
	l.h.registrar(lat)
	if sub < m.r.Config.Lentos {
		m.lentos.registrar(lat)
	} else {
		m.normais.registrar(lat)
	}
}

func (m *coletor) avisos(ctx context.Context, c *client.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.Eventos():
			if !ok {
				return
			}
			m.Lock()
			l := m.linha(time.Now())
			switch msg.Tipo {
			case protocol.TipoDescartadas:
				n, _ := strconv.ParseUint(string(msg.Conteudo), 10, 64)
				m.r.DescartesBroker += n
			case protocol.TipoSemInscritos:
				m.r.SemInscritos++
				l.SemInscritos++
			case protocol.TipoErro:
				if string(msg.Conteudo) == "RATE_LIMIT" {
					m.r.Rejeitadas++
					l.Rejeitadas++
				} else {
					m.r.OutrosErros++
				}
			}
			m.Unlock()
		}
	}
}
