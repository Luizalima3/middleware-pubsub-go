package broker

import "sync"

// itemFila é uma entrega pendente para um inscrito: o frame TipoMensagem já
// codificado (compartilhado, nunca alterado) e o tópico de origem.
type itemFila struct {
	topico *Topico
	frame  []byte
}

// filaDados é o buffer circular limitado de entregas de uma sessão.
//
// Política drop-oldest: inserir nunca bloqueia. Com a fila cheia, a entrega
// mais antiga sai, a nova entra e o contador de descartes do tópico da entrega
// removida sobe. No cenário C a mensagem mais recente é a mais valiosa.
//
// Um único consumidor (o escritor da sessão) espera em Sinal() quando a fila
// está vazia. O canal de sinalização tem capacidade 1, então várias inserções
// seguidas geram no máximo um despertar pendente, e nada se perde porque o
// consumidor sempre volta a chamar Retirar depois de acordar.
type filaDados struct {
	mu        sync.Mutex
	itens     []itemFila
	inicio    int // posição do item mais antigo
	tam       int // quantidade de itens válidos
	descartes map[string]uint32
	sinal     chan struct{}
}

func novaFilaDados(capacidade int) *filaDados {
	if capacidade < 1 {
		capacidade = 1
	}
	return &filaDados{
		itens:     make([]itemFila, capacidade),
		descartes: make(map[string]uint32),
		sinal:     make(chan struct{}, 1),
	}
}

// Inserir coloca o item no fim da fila. Se a fila estava cheia, devolve o item
// mais antigo, que foi descartado para abrir espaço, e descartou = true.
func (f *filaDados) Inserir(it itemFila) (removido itemFila, descartou bool) {
	f.mu.Lock()
	capacidade := len(f.itens)
	if f.tam == capacidade {
		removido = f.itens[f.inicio]
		descartou = true
		f.itens[f.inicio] = itemFila{}
		f.inicio = (f.inicio + 1) % capacidade
		f.tam--
		nome := ""
		if removido.topico != nil {
			nome = removido.topico.nome
		}
		f.descartes[nome]++
	}
	f.itens[(f.inicio+f.tam)%capacidade] = it
	f.tam++
	f.mu.Unlock()

	select {
	case f.sinal <- struct{}{}:
	default: // já há um despertar pendente
	}
	return removido, descartou
}

// Retirar remove e devolve o item mais antigo, sem bloquear.
func (f *filaDados) Retirar() (itemFila, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.tam == 0 {
		return itemFila{}, false
	}
	it := f.itens[f.inicio]
	f.itens[f.inicio] = itemFila{} // libera o frame para o coletor de lixo
	f.inicio = (f.inicio + 1) % len(f.itens)
	f.tam--
	return it, true
}

// Sinal devolve o canal que recebe um aviso a cada inserção (coalescido).
func (f *filaDados) Sinal() <-chan struct{} { return f.sinal }

// Tamanho devolve quantos itens aguardam na fila.
func (f *filaDados) Tamanho() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tam
}

// Capacidade devolve o limite da fila.
func (f *filaDados) Capacidade() int { return len(f.itens) }

// TemDescartes informa se há descartes ainda não avisados ao inscrito.
func (f *filaDados) TemDescartes() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.descartes) > 0
}

// TomarDescartes devolve a contagem de descartes por tópico desde a última
// chamada e zera os contadores.
func (f *filaDados) TomarDescartes() map[string]uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.descartes) == 0 {
		return nil
	}
	d := f.descartes
	f.descartes = make(map[string]uint32)
	return d
}
