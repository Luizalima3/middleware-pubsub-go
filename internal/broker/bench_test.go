package broker

import (
	"fmt"
	"testing"

	"github.com/Luizalima3/middleware-pubsub-go/internal/fila"
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
)

// Adapta o benchmark do núcleo WebSocket para as filas usadas pelo broker UDP.
// Mede somente inserção nas filas de 10, 100 e 1000 destinos, sem rede,
// despachante, confirmação ou controle de taxa. Não mede capacidade do servidor.
func BenchmarkFanout(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("inscritos_%d", n), func(b *testing.B) {
			m := protocol.Mensagem{Tipo: protocol.TipoMensagem, ID: 1, Cliente: "bench", Topico: "bench/fanout", Conteudo: []byte("Gol!")}
			dados, err := protocol.Codificar(m)
			if err != nil {
				b.Fatal(err)
			}
			e := entrega{m: m, dados: dados}
			filas := make([]*fila.Fila[entrega], n)
			for i := range filas {
				filas[i] = fila.Nova[entrega](64, 64000)
				// Mede filas cheias, exercitando o descarte do mais antigo.
				for j := 0; j < 64; j++ {
					filas[i].Adicionar(e, len(dados), true)
				}
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(dados) * n))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, f := range filas {
					f.Adicionar(e, len(dados), true)
				}
			}
		})
	}
}
