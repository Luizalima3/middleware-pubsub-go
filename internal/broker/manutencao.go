package broker

import (
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
	"strconv"
	"time"
)

func (b *Broker) comunicarTopico(tipo protocol.Tipo, nome string) {
	if b.par != nil {
		b.controle(protocol.Mensagem{Tipo: tipo, Topico: nome, Origem: b.cfg.ID}, b.par, true)
	}
}
func (b *Broker) anunciar(nome string) {
	if b.par == nil {
		return
	}
	b.mu.Lock()
	n := 0
	if t := b.topicos[nome]; t != nil {
		n = len(t.inscritos)
	}
	b.mu.Unlock()
	b.controle(protocol.Mensagem{Tipo: protocol.TipoInteresse, Topico: nome, Fila: n, Origem: b.cfg.ID}, b.par, false)
}
func (b *Broker) receberPar(m protocol.Mensagem) {
	// O endereço de origem já foi conferido contra o único par configurado.
	if m.Tipo != protocol.TipoRepasse && !b.controleEntrada.Permitir() {
		return
	}
	if m.Tipo == protocol.TipoInteresse {
		if !protocol.TopicoValido(m.Topico) {
			return
		}
		b.mu.Lock()
		if m.Fila > 0 && len(b.interesses) < b.cfg.MaxTopicos {
			b.interesses[m.Topico] = time.Now()
			b.novoTopico(m.Topico)
		} else if m.Fila <= 0 {
			delete(b.interesses, m.Topico)
		}
		b.mu.Unlock()
		return
	}
	if m.Tipo == protocol.TipoCriarRemoto || m.Tipo == protocol.TipoApagarRemoto {
		if m.Aviso == 0 || !protocol.TopicoValido(m.Topico) {
			return
		}
		key := "par:" + strconv.FormatUint(m.Aviso, 10)
		if _, ok := b.avisos.Buscar(key); !ok {
			if m.Tipo == protocol.TipoApagarRemoto {
				b.apagar(m.Topico)
			} else {
				b.mu.Lock()
				b.novoTopico(m.Topico)
				b.mu.Unlock()
			}
			b.avisos.Guardar(key, true)
		}
		b.controle(protocol.Mensagem{Tipo: protocol.TipoRecebido, Aviso: m.Aviso}, b.par, false)
		return
	}
	if m.Tipo != protocol.TipoRepasse || !protocol.TopicoValido(m.Topico) || m.Cliente == "" || m.ID == 0 {
		return
	}
	if !b.entradaPar.Permitir() || !b.entrada.Permitir() {
		b.cont.rejeitadas.Add(1)
		return
	}
	if _, ok := b.vistos.Buscar(chave(m)); ok {
		b.cont.duplicadas.Add(1)
		return
	}
	b.mu.Lock()
	t := b.topicos[m.Topico]
	b.mu.Unlock()
	if t == nil {
		return
	}
	dados, e := protocol.Codificar(m)
	if e != nil {
		return
	}
	if _, ok := t.fila.Adicionar(entrega{m, dados}, len(dados), false); ok {
		b.vistos.Guardar(chave(m), true)
	} else {
		b.cont.descartadas.Add(1)
	}
}
func (b *Broker) manter() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.cfg.Intervalo)
	retry := time.NewTicker(b.cfg.Repeticao)
	defer ticker.Stop()
	defer retry.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-retry.C:
			b.repetir()
		case <-ticker.C:
			b.avaliar()
		}
	}
}
func (b *Broker) repetir() {
	agora := time.Now()
	var envios []saida
	b.mu.Lock()
	for key, p := range b.pendentes {
		if agora.Before(p.proxima) {
			continue
		}
		if p.tentativas >= b.cfg.Tentativas {
			delete(b.pendentes, key)
			b.cont.controlePerdido.Add(1)
			continue
		}
		p.tentativas++
		p.proxima = agora.Add(b.cfg.Repeticao)
		envios = append(envios, saida{m: p.m, addr: p.addr})
	}
	b.mu.Unlock()
	for _, s := range envios {
		select {
		case b.controles <- s:
		default:
			b.cont.controlePerdido.Add(1)
		}
	}
}
func (b *Broker) avaliar() {
	agora := time.Now()
	type mensagem struct {
		s *sessao
		m protocol.Mensagem
	}
	var avisos []mensagem
	var temas []string
	b.mu.Lock()
	for nome, t := range b.topicos {
		temas = append(temas, nome)
		_ = t
	}
	ativos := map[string]bool{}
	for _, s := range b.clientes {
		if s.publicador && agora.Sub(s.visto) < b.cfg.Inatividade {
			ativos[s.id] = true
		}
	}
	n := len(ativos)
	if n < 1 {
		n = 1
	}
	teto := b.cfg.TaxaEntrada / float64(n)
	if teto > b.cfg.TaxaCliente {
		teto = b.cfg.TaxaCliente
	}
	pressaoFila := false
	for _, t := range b.topicos {
		q, _ := t.fila.Tamanho()
		if q*2 >= b.cfg.FilaTopico {
			pressaoFila = true
		}
	}
	for key, s := range b.clientes {
		// A manutenção pode ser mais frequente que o feedback do cliente.
		// Não confundir o período de limpeza/anúncios com o prazo de inatividade.
		if agora.Sub(s.feedbackEm) > b.cfg.Inatividade/2 {
			s.taxa.Avaliar(true, 0)
		}
		if agora.Sub(s.visto) >= b.cfg.Inatividade {
			s.cancel()
			s.fila.Limpar()
			delete(b.clientes, key)
			for _, t := range b.topicos {
				delete(t.inscritos, key)
			}
			continue
		}
		if q := b.cotas[s.id]; q != nil {
			_ = q.bucket.DefinirTaxa(teto)
		}
		if pressaoFila {
			s.pressao = true
		}
		for tema, q := range s.descartes {
			if q > 0 {
				avisos = append(avisos, mensagem{s, protocol.Mensagem{Tipo: protocol.TipoDescartadas, Topico: tema, Conteudo: []byte(strconv.FormatUint(q, 10))}})
			}
			delete(s.descartes, tema)
		}
	}
	for id, q := range b.cotas {
		if agora.Sub(q.visto) > 2*b.cfg.Inatividade {
			usado := false
			for _, s := range b.clientes {
				if s.id == id {
					usado = true
					break
				}
			}
			if !usado {
				delete(b.cotas, id)
			}
		}
	}
	for nome, v := range b.interesses {
		if agora.Sub(v) > b.cfg.Inatividade {
			delete(b.interesses, nome)
		}
	}
	b.mu.Unlock()
	for _, v := range avisos {
		if v.m.Tipo == protocol.TipoEstado {
			b.controle(v.m, v.s.addr, false)
		} else {
			b.aviso(v.s, v.m)
		}
	}
	for _, nome := range temas {
		b.anunciar(nome)
	}
}
