package controle

import "github.com/Luizalima3/middleware-pubsub-go/internal/protocol"

// Verificar deve ser chamado no broker antes de encaminhar uma publicação.
// nil autoriza a operação. Uma resposta não nil deve ser enfileirada para o
// publicador e a publicação original não deve seguir para o núcleo.
// Inscrições e outras operações não gastam tokens deste limite.
func (b *Bucket) Verificar(m protocol.Mensagem) *protocol.Mensagem {
	if m.Tipo != protocol.TipoPublicar || b.Permitir() {
		return nil
	}
	return &protocol.Mensagem{Tipo: protocol.TipoErro, ID: m.ID, Topico: m.Topico, Conteudo: []byte("RATE_LIMIT")}
}
