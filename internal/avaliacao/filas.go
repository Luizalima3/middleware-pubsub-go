package avaliacao

import (
	"context"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/client"
)

func (m *coletor) amostrarFilas(ctx context.Context, admin *client.Client, subs, pubs []*client.Client) {
	amostrar := func() {
		now := time.Now()
		if now.Before(m.inicio) {
			return
		}
		m.Lock()
		defer m.Unlock()
		registrar := func(papel string, i int, c *client.Client) {
			q := c.Filas()
			m.filas = append(m.filas, LinhaFila{now, now.Sub(m.inicio).Seconds(), papel, i, "dados", q.Dados, q.LimiteDados}, LinhaFila{now, now.Sub(m.inicio).Seconds(), papel, i, "avisos", q.Avisos, q.LimiteAvisos})
		}
		registrar("admin", 0, admin)
		for i, c := range subs {
			registrar("assinante", i, c)
		}
		for i, c := range pubs {
			registrar("publicador", i, c)
		}
		for i, q := range m.filasEnvio {
			if q != nil {
				m.filas = append(m.filas, LinhaFila{now, now.Sub(m.inicio).Seconds(), "publicador", i, "gerador", len(q), cap(q)})
			}
		}
	}
	ticker := time.NewTicker(m.r.Config.IntervaloMetricas)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			amostrar()
			return
		case <-ticker.C:
			amostrar()
		}
	}
}
