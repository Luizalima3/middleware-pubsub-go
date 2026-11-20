package broker

import (
	"sync"
	"sync/atomic"
)

// Topico guarda os inscritos de um tópico, a fila limitada de publicações e a
// goroutine despachante que faz o fan-out.
//
// Dois níveis de buffer: a fila do tópico absorve rajadas sem bloquear o
// leitor do publicador; a fila de dados de cada inscrito isola o consumidor
// lento (ver Sessao).
type Topico struct {
	nome string
	b    *Broker

	// fila recebe os frames TipoMensagem já codificados. Nunca é fechada:
	// leitores podem estar enviando enquanto o tópico é apagado.
	fila chan []byte

	// mu serializa as mudanças na lista de inscritos e o flag apagado.
	mu      sync.Mutex
	apagado bool
	// inscritos é substituída por cópia (copy-on-write) sob mu; o despachante
	// lê o snapshot sem lock.
	inscritos atomic.Pointer[[]*Sessao]

	fim       chan struct{} // fechado ao apagar o tópico ou encerrar o broker
	fimUmaVez sync.Once

	publicacoes atomic.Uint64
	descartes   atomic.Uint64
}

func novoTopico(b *Broker, nome string) *Topico {
	t := &Topico{
		nome: nome,
		b:    b,
		fila: make(chan []byte, b.cfg.FilaTopico),
		fim:  make(chan struct{}),
	}
	vazia := []*Sessao{}
	t.inscritos.Store(&vazia)
	return t
}

// Nome devolve o nome do tópico.
func (t *Topico) Nome() string { return t.nome }

// enfileirar coloca uma publicação na fila do tópico sem bloquear.
// Devolve false se a fila estiver cheia (a publicação deve ser descartada).
func (t *Topico) enfileirar(frame []byte) bool {
	select {
	case t.fila <- frame:
		t.publicacoes.Add(1)
		return true
	default:
		return false
	}
}

// despachar é a goroutine do tópico: retira cada publicação da fila e a
// entrega na fila de dados de cada inscrito. Nunca escreve em socket e nunca
// bloqueia num inscrito, pois a inserção na fila de dados é drop-oldest.
func (t *Topico) despachar() {
	defer t.b.wg.Done()
	for {
		select {
		case <-t.fim:
			return
		case frame := <-t.fila:
			for _, s := range *t.inscritos.Load() {
				s.entregar(t, frame)
			}
		}
	}
}

// adicionar inscreve a sessão. Devolve false se o tópico já foi apagado.
// Idempotente: inscrever de novo a mesma sessão não duplica entregas.
//
// A inscrição também é registrada na sessão ainda sob t.mu, na ordem de locks
// t.mu → s.mu (a mesma de apagar), para que um ApagarTopico concorrente nunca
// deixe a sessão apontando para um tópico morto.
func (t *Topico) adicionar(s *Sessao) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.apagado {
		return false
	}
	atual := *t.inscritos.Load()
	jaInscrito := false
	for _, x := range atual {
		if x == s {
			jaInscrito = true
			break
		}
	}
	if !jaInscrito {
		nova := make([]*Sessao, len(atual), len(atual)+1)
		copy(nova, atual)
		nova = append(nova, s)
		t.inscritos.Store(&nova)
	}
	s.mu.Lock()
	s.inscricoes[t.nome] = t
	s.mu.Unlock()
	return true
}

// remover cancela a inscrição da sessão, se houver. Não mexe no mapa da
// sessão; quem chama cuida disso.
func (t *Topico) remover(s *Sessao) {
	t.mu.Lock()
	defer t.mu.Unlock()
	atual := *t.inscritos.Load()
	for i, x := range atual {
		if x == s {
			nova := make([]*Sessao, 0, len(atual)-1)
			nova = append(nova, atual[:i]...)
			nova = append(nova, atual[i+1:]...)
			t.inscritos.Store(&nova)
			return
		}
	}
}

// apagar marca o tópico como apagado, para o despachante e devolve os
// inscritos que existiam, já desligados do tópico. As publicações que ainda
// estavam na fila do tópico são descartadas.
func (t *Topico) apagar() []*Sessao {
	t.mu.Lock()
	t.apagado = true
	antigos := *t.inscritos.Load()
	vazia := []*Sessao{}
	t.inscritos.Store(&vazia)
	for _, s := range antigos {
		s.mu.Lock()
		if s.inscricoes[t.nome] == t {
			delete(s.inscricoes, t.nome)
		}
		s.mu.Unlock()
	}
	t.mu.Unlock()
	t.parar()
	return antigos
}

// parar encerra o despachante (idempotente).
func (t *Topico) parar() {
	t.fimUmaVez.Do(func() { close(t.fim) })
}
