package avaliacao

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/protocol"
)

// Um broker UDP de teste injeta cada entrega três vezes. A biblioteca entrega
// apenas uma à aplicação; o resumo deve enxergar também as outras duas.
func TestDuplicatasFiltradasPeloClienteChegamAoResumo(t *testing.T) {
	s, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		var sub *net.UDPAddr
		buf := make([]byte, 1001)
		enviar := func(m protocol.Mensagem, a *net.UDPAddr) {
			d, e := protocol.Codificar(m)
			if e == nil {
				s.WriteToUDP(d, a)
			}
		}
		for {
			n, a, e := s.ReadFromUDP(buf)
			if e != nil {
				return
			}
			m, e := protocol.Decodificar(buf[:n])
			if e != nil {
				continue
			}
			switch m.Tipo {
			case protocol.TipoAtividade:
				enviar(protocol.Mensagem{Tipo: protocol.TipoEstado, ID: m.ID, Taxa: 100}, a)
			case protocol.TipoCriarTopico, protocol.TipoAssinar:
				if m.Tipo == protocol.TipoAssinar {
					sub = a
				}
				enviar(protocol.Mensagem{Tipo: protocol.TipoOK, ID: m.ID, Topico: m.Topico}, a)
			case protocol.TipoPublicar:
				m.Tipo = protocol.TipoMensagem
				if sub != nil {
					for i := 0; i < 3; i++ {
						enviar(m, sub)
					}
				}
			}
		}
	}()
	defer func() { s.Close(); <-done }()
	cfg := Config{URLs: []string{s.LocalAddr().String()}, Topico: "teste/duplicatas", Publicadores: 1, Assinantes: 1, Taxa: 10, Duracao: 300 * time.Millisecond, Drenagem: 300 * time.Millisecond, Timeout: time.Second, Payload: 128, Fila: 8}
	r, e := Executar(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	if r.Resumo.Enviadas != 3 || r.Resumo.Recebidas != 3 || r.Resumo.DuplicadasCliente != 6 || r.Resumo.DuplicadasGerador != 0 || r.Resumo.Duplicadas != 6 {
		t.Fatalf("%+v", r.Resumo)
	}
	if len(r.Filas) == 0 {
		t.Fatal("não amostrou filas locais")
	}
}
