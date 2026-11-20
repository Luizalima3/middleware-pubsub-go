package broker

import (
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
	"testing"
)

// Dados do par não devem consumir as fichas reservadas a comandos e atividade.
func TestRepasseNaoConsomeOrcamentoDeControle(t *testing.T) {
	b, e := Iniciar(Config{Endereco: "127.0.0.1:0", TaxaControle: .001, Rajada: 2})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Fechar()
	for i := 1; i <= 10; i++ {
		b.receberPar(protocol.Mensagem{Tipo: protocol.TipoRepasse, ID: uint64(i), Cliente: "par", Topico: "ausente"})
	}
	if !b.controleEntrada.Permitir() || !b.controleEntrada.Permitir() || b.controleEntrada.Permitir() {
		t.Fatal("dados alteraram o orçamento administrativo")
	}
}
