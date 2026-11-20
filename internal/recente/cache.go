// Package recente guarda IDs por tempo e quantidade limitados, sem histórico.
package recente

import (
	"sync"
	"time"
)

type registro[T any] struct {
	valor  T
	expira time.Time
	versao uint64
}
type posicao struct {
	chave  string
	versao uint64
}
type Cache[T any] struct {
	mu      sync.Mutex
	itens   map[string]registro[T]
	ordem   []posicao
	proximo int
	versao  uint64
	ttl     time.Duration
}

func Nova[T any](capacidade int, ttl time.Duration) *Cache[T] {
	if capacidade < 1 {
		panic("capacidade inválida")
	}
	return &Cache[T]{itens: make(map[string]registro[T]), ordem: make([]posicao, capacidade), ttl: ttl}
}
func (c *Cache[T]) Buscar(chave string) (T, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.itens[chave]
	if ok && time.Now().Before(r.expira) {
		return r.valor, true
	}
	delete(c.itens, chave)
	var zero T
	return zero, false
}
func (c *Cache[T]) Guardar(chave string, valor T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.itens[chave]; ok && time.Now().Before(r.expira) {
		r.valor = valor
		c.itens[chave] = r
		return
	}
	a := c.ordem[c.proximo]
	if r, ok := c.itens[a.chave]; ok && r.versao == a.versao {
		delete(c.itens, a.chave)
	}
	c.versao++
	c.itens[chave] = registro[T]{valor, time.Now().Add(c.ttl), c.versao}
	c.ordem[c.proximo] = posicao{chave, c.versao}
	c.proximo = (c.proximo + 1) % len(c.ordem)
}
