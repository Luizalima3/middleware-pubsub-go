package controle

import (
	"github.com/Luizalima3/middleware-pubsub-go/internal/protocol"
	"testing"
)

func TestFiltroPreservaComandosEIdentificaRejeicao(t *testing.T) {
	b, _ := Novo(.0001, 1)
	if b.Verificar(protocol.Mensagem{Tipo: protocol.TipoAssinar}) != nil {
		t.Fatal("inscrição limitada")
	}
	m := protocol.Mensagem{Tipo: protocol.TipoPublicar, ID: 42, Topico: "evento/reacoes"}
	if b.Verificar(m) != nil {
		t.Fatal("primeira publicação recusada")
	}
	r := b.Verificar(m)
	if r == nil || r.Tipo != protocol.TipoErro || r.ID != 42 || r.Topico != m.Topico || string(r.Conteudo) != "RATE_LIMIT" {
		t.Fatalf("resposta: %+v", r)
	}
}
