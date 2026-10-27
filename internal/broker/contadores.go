package broker

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

// contadores agrupa os contadores globais do núcleo. Todos são atômicos para
// serem atualizados nos caminhos quentes sem lock.
type contadores struct {
	conexoesAtivas                     atomic.Int64
	publicacoesRecebidas               atomic.Uint64
	publicacoesRejeitadasRateLimit     atomic.Uint64
	publicacoesSemInscritos            atomic.Uint64
	publicacoesTopicoInexistente       atomic.Uint64
	publicacoesDescartadasFilaTopico   atomic.Uint64
	entregasEnfileiradas               atomic.Uint64
	entregasEscritas                   atomic.Uint64
	entregasDescartadasConsumidorLento atomic.Uint64
	avisosControleDescartados          atomic.Uint64
	sessoesEncerradasTimeoutEscrita    atomic.Uint64
}

// ContadoresTopico é a fotografia dos contadores de um tópico.
type ContadoresTopico struct {
	// Inscritos é o número de sessões inscritas no momento.
	Inscritos int `json:"inscritos"`
	// Publicacoes conta as publicações aceitas na fila do tópico.
	Publicacoes uint64 `json:"publicacoes"`
	// Descartes conta entregas deste tópico removidas por drop-oldest nas
	// filas de inscritos lentos.
	Descartes uint64 `json:"descartes"`
	// OcupacaoFila é quantas publicações aguardam o despachante.
	OcupacaoFila int `json:"ocupacao_fila"`
}

// Contadores é a fotografia dos contadores do núcleo, exposta em
// GET /contadores. Serve para comprovar o funcionamento do núcleo; métricas de
// desempenho e gráficos são responsabilidade da Parte 3.
type Contadores struct {
	ConexoesAtivas                     int64                       `json:"conexoes_ativas"`
	TopicosAtivos                      int                         `json:"topicos_ativos"`
	PublicacoesRecebidas               uint64                      `json:"publicacoes_recebidas"`
	PublicacoesRejeitadasRateLimit     uint64                      `json:"publicacoes_rejeitadas_rate_limit"`
	PublicacoesSemInscritos            uint64                      `json:"publicacoes_sem_inscritos"`
	PublicacoesTopicoInexistente       uint64                      `json:"publicacoes_topico_inexistente"`
	PublicacoesDescartadasFilaTopico   uint64                      `json:"publicacoes_descartadas_fila_topico"`
	EntregasEnfileiradas               uint64                      `json:"entregas_enfileiradas"`
	EntregasEscritas                   uint64                      `json:"entregas_escritas"`
	EntregasDescartadasConsumidorLento uint64                      `json:"entregas_descartadas_consumidor_lento"`
	AvisosControleDescartados          uint64                      `json:"avisos_controle_descartados"`
	SessoesEncerradasTimeoutEscrita    uint64                      `json:"sessoes_encerradas_timeout_escrita"`
	Topicos                            map[string]ContadoresTopico `json:"topicos"`
}

// Contadores devolve uma fotografia dos contadores do núcleo. Os valores são
// lidos um a um, sem pausa global, então podem não ser mutuamente consistentes
// num mesmo instante; servem para observação, não para contabilidade exata.
func (b *Broker) Contadores() Contadores {
	c := &b.cont
	r := Contadores{
		ConexoesAtivas:                     c.conexoesAtivas.Load(),
		PublicacoesRecebidas:               c.publicacoesRecebidas.Load(),
		PublicacoesRejeitadasRateLimit:     c.publicacoesRejeitadasRateLimit.Load(),
		PublicacoesSemInscritos:            c.publicacoesSemInscritos.Load(),
		PublicacoesTopicoInexistente:       c.publicacoesTopicoInexistente.Load(),
		PublicacoesDescartadasFilaTopico:   c.publicacoesDescartadasFilaTopico.Load(),
		EntregasEnfileiradas:               c.entregasEnfileiradas.Load(),
		EntregasEscritas:                   c.entregasEscritas.Load(),
		EntregasDescartadasConsumidorLento: c.entregasDescartadasConsumidorLento.Load(),
		AvisosControleDescartados:          c.avisosControleDescartados.Load(),
		SessoesEncerradasTimeoutEscrita:    c.sessoesEncerradasTimeoutEscrita.Load(),
	}

	b.mu.RLock()
	topicos := make([]*Topico, 0, len(b.topicos))
	for _, t := range b.topicos {
		topicos = append(topicos, t)
	}
	b.mu.RUnlock()

	r.TopicosAtivos = len(topicos)
	r.Topicos = make(map[string]ContadoresTopico, len(topicos))
	for _, t := range topicos {
		r.Topicos[t.nome] = ContadoresTopico{
			Inscritos:    len(*t.inscritos.Load()),
			Publicacoes:  t.publicacoes.Load(),
			Descartes:    t.descartes.Load(),
			OcupacaoFila: len(t.fila),
		}
	}
	return r
}

// tratarContadores responde GET /contadores com o JSON dos contadores.
func (b *Broker) tratarContadores(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "método não permitido", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(b.Contadores())
}
