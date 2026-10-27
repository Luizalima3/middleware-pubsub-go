package broker

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestFilaDadosCapacidadeOrdemEDropOldest(t *testing.T) {
	f := novaFilaDados(4)
	if f.Capacidade() != 4 {
		t.Fatalf("capacidade esperada 4, obteve %d", f.Capacidade())
	}

	topA := &Topico{nome: "evento/a"}
	topB := &Topico{nome: "evento/b"}

	// Preenche até a capacidade sem descartar.
	for i := 1; i <= 4; i++ {
		top := topA
		if i == 2 {
			top = topB
		}
		_, desc := f.Inserir(itemFila{topico: top, frame: []byte{byte(i)}})
		if desc {
			t.Fatalf("descarte inesperado na inserção %d", i)
		}
	}
	if f.Tamanho() != 4 || f.TemDescartes() {
		t.Fatalf("estado inesperado: tam=%d descartes=%v", f.Tamanho(), f.TemDescartes())
	}

	// Inserir mais 2 deve descartar os 2 mais antigos (1 de topA e 2 de topB).
	for i := 5; i <= 6; i++ {
		rem, desc := f.Inserir(itemFila{topico: topA, frame: []byte{byte(i)}})
		if !desc {
			t.Fatalf("esperava descarte na inserção %d", i)
		}
		if int(rem.frame[0]) != i-4 {
			t.Fatalf("esperava remover %d, removeu %d", i-4, rem.frame[0])
		}
	}
	if f.Tamanho() != 4 {
		t.Fatalf("capacidade não respeitada: tam=%d", f.Tamanho())
	}

	// Ordem FIFO dos sobreviventes: 3, 4, 5, 6.
	for esperado := 3; esperado <= 6; esperado++ {
		it, ok := f.Retirar()
		if !ok || int(it.frame[0]) != esperado {
			t.Fatalf("esperava %d, obteve ok=%v val=%v", esperado, ok, it.frame)
		}
	}
	if _, ok := f.Retirar(); ok {
		t.Fatal("fila deveria estar vazia")
	}

	// Contagem de descartes por tópico do item que saiu (1 de topA, 1 de topB).
	desc := f.TomarDescartes()
	if desc["evento/a"] != 1 || desc["evento/b"] != 1 {
		t.Fatalf("descartes por tópico inesperados: %+v", desc)
	}
	if f.TemDescartes() || f.TomarDescartes() != nil {
		t.Fatal("descartes deveriam ter sido zerados após TomarDescartes")
	}
}

func TestFilaDadosInserirNuncaBloqueiaEConcorrencia(t *testing.T) {
	f := novaFilaDados(16)
	top := &Topico{nome: "evento/concorrente"}

	// Sem nenhum consumidor lendo Sinal() nem chamando Retirar(),
	// milhares de inserções concorrentes devem terminar imediatamente.
	pronto := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < 500; i++ {
					f.Inserir(itemFila{topico: top, frame: []byte(fmt.Sprintf("%d-%d", id, i))})
				}
			}(g)
		}
		wg.Wait()
		close(pronto)
	}()

	select {
	case <-pronto:
	case <-time.After(2 * time.Second):
		t.Fatal("Inserir bloqueou sem consumidor")
	}

	if f.Tamanho() != 16 {
		t.Fatalf("esperava fila cheia com 16 itens, obteve %d", f.Tamanho())
	}
	desc := f.TomarDescartes()
	if desc["evento/concorrente"] != 8*500-16 {
		t.Fatalf("esperava %d descartes, obteve %d", 8*500-16, desc["evento/concorrente"])
	}
}
