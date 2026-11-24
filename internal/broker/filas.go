package broker

import (
	"sort"

	"github.com/Luizalima3/middleware-pubsub-go/internal/fila"
	"github.com/Luizalima3/middleware-pubsub-go/internal/metricas"
)

// Filas lê cada fila sob seu mutex. Não é uma transação entre todas as filas.
// Controle guarda datagramas com tamanhos variados; bytes não são medidos ali.
func (b *Broker) Filas() []metricas.Fila {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := []metricas.Fila{
		{Tipo: "controle", Nome: "global", Estado: fila.Estado{Itens: len(b.controles), LimiteItens: cap(b.controles)}},
		{Tipo: "par", Nome: "par", Estado: b.filaPar.Estado()},
	}
	for nome, t := range b.topicos {
		out = append(out, metricas.Fila{Tipo: "topico", Nome: nome, Estado: t.fila.Estado()})
	}
	for endpoint, c := range b.clientes {
		envio := 0
		if c.emEnvio {
			envio = 1
		}
		out = append(out, metricas.Fila{Tipo: "consumidor", Nome: c.id + "@" + endpoint, Estado: c.fila.Estado(), EmEnvio: envio})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tipo == out[j].Tipo {
			return out[i].Nome < out[j].Nome
		}
		return out[i].Tipo < out[j].Tipo
	})
	return out
}
