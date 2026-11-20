package broker

import (
	"fmt"
	"testing"

	"github.com/Luizalima3/middleware-pubsub-go/internal/protocol"
)

// BenchmarkFanout mede o caminho quente de entrega a 10, 100 e 1000 inscritos
// por tópico (1 publicador codifica uma única vez e enfileira nos inscritos).
func BenchmarkFanout(b *testing.B) {
	for _, numInscritos := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("inscritos_%d", numInscritos), func(b *testing.B) {
			brk := Novo(Config{
				FilaAssinante: 4096,
				FilaTopico:    8192,
				Silencioso:    true,
			})
			defer brk.Fechar()

			top := novoTopico(brk, "bench/fanout")
			sessoes := make([]*Sessao, numInscritos)
			for i := 0; i < numInscritos; i++ {
				s := &Sessao{
					id:         uint64(i + 1),
					b:          brk,
					dados:      novaFilaDados(4096),
					inscricoes: make(map[string]*Topico),
				}
				sessoes[i] = s
				top.adicionar(s)
			}

			frame, err := protocol.Codificar(protocol.Mensagem{
				Tipo:     protocol.TipoMensagem,
				ID:       1,
				Topico:   "bench/fanout",
				Conteudo: make([]byte, 128),
			})
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(frame) * numInscritos))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, s := range *top.inscritos.Load() {
					s.entregar(top, frame)
				}
			}
		})
	}
}
