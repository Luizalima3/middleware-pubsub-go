// Package fila implementa filas limitadas por itens e bytes.
package fila

import "sync"

type item[T any] struct {
	valor T
	bytes int
}
type Fila[T any] struct {
	mu                        sync.Mutex
	itens                     []item[T]
	bytes, maxItens, maxBytes int
	picoItens, picoBytes      int
	Sinal                     chan struct{}
}

func Nova[T any](itens, bytes int) *Fila[T] {
	if itens < 1 || bytes < 1 {
		panic("limites inválidos")
	}
	return &Fila[T]{maxItens: itens, maxBytes: bytes, Sinal: make(chan struct{}, 1)}
}

// Adicionar recusa a entrada ou retira os itens mais antigos, conforme a política.
func (f *Fila[T]) Adicionar(v T, n int, descartarAntigos bool) ([]T, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var removidos []T
	if n < 0 || n > f.maxBytes {
		return nil, false
	}
	for len(f.itens) >= f.maxItens || f.bytes+n > f.maxBytes {
		if !descartarAntigos {
			return nil, false
		}
		r := f.itens[0]
		f.itens[0] = item[T]{}
		f.itens = f.itens[1:]
		f.bytes -= r.bytes
		removidos = append(removidos, r.valor)
	}
	f.itens = append(f.itens, item[T]{v, n})
	f.bytes += n
	if len(f.itens) > f.picoItens {
		f.picoItens = len(f.itens)
	}
	if f.bytes > f.picoBytes {
		f.picoBytes = f.bytes
	}
	select {
	case f.Sinal <- struct{}{}:
	default:
	}
	return removidos, true
}
func (f *Fila[T]) Retirar() (T, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var zero T
	if len(f.itens) == 0 {
		return zero, false
	}
	r := f.itens[0]
	f.itens[0] = item[T]{}
	f.itens = f.itens[1:]
	f.bytes -= r.bytes
	return r.valor, true
}
func (f *Fila[T]) Tamanho() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.itens), f.bytes
}

// Estado é uma fotografia da fila. Picos são acumulados desde sua criação,
// inclusive entre amostras, e não incluem itens já retirados para envio.
type Estado struct {
	Itens, Bytes, LimiteItens, LimiteBytes, PicoItens, PicoBytes int
}

func (f *Fila[T]) Estado() Estado {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Estado{len(f.itens), f.bytes, f.maxItens, f.maxBytes, f.picoItens, f.picoBytes}
}
func (f *Fila[T]) Limpar() { f.mu.Lock(); defer f.mu.Unlock(); f.itens = nil; f.bytes = 0 }
func (f *Fila[T]) Remover(se func(T) bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	novos := f.itens[:0]
	total := 0
	for _, it := range f.itens {
		if !se(it.valor) {
			novos = append(novos, it)
			total += it.bytes
		}
	}
	for i := len(novos); i < len(f.itens); i++ {
		f.itens[i] = item[T]{}
	}
	f.itens = novos
	f.bytes = total
}
