// Package controle limita publicações sem bloquear os demais clientes.
package controle

import (
	"errors"
	"math"
	"sync"
	"time"
)

// Bucket deve ser compartilhado pelas sessões de um identificador de cliente no broker.
// Um token corresponde a uma publicação; o tamanho máximo do frame continua
// sendo validado pelo protocolo. Um novo bucket começa com a rajada completa; mudar sua taxa não o reinicia.
type Bucket struct {
	mu                       sync.Mutex
	taxa, capacidade, tokens float64
	ultimo                   time.Time
	aceitas, rejeitadas      uint64
}

func Novo(taxa float64, rajada int) (*Bucket, error) {
	if taxa <= 0 || math.IsNaN(taxa) || math.IsInf(taxa, 0) || rajada < 1 {
		return nil, errors.New("taxa deve ser finita e positiva; rajada deve ser >= 1")
	}
	return &Bucket{taxa: taxa, capacidade: float64(rajada), tokens: float64(rajada)}, nil
}

// Permitir não espera por tokens. O broker rejeita o excedente antes do fan-out.
func (b *Bucket) Permitir() bool { return b.PermitirEm(time.Now()) }

// PermitirEm permite testes determinísticos. Instantes antigos não repõem tokens.
func (b *Bucket) PermitirEm(agora time.Time) bool {
	return b.PermitirNEm(1, agora)
}

// PermitirNEm também permite limitar bytes; não espera por fichas.
func (b *Bucket) PermitirNEm(n float64, agora time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n <= 0 || math.IsNaN(n) || math.IsInf(n, 0) {
		return false
	}
	if b.ultimo.IsZero() {
		b.ultimo = agora
	}
	if agora.After(b.ultimo) {
		b.tokens = math.Min(b.capacidade, b.tokens+agora.Sub(b.ultimo).Seconds()*b.taxa)
		b.ultimo = agora
	}
	if b.tokens < n {
		b.rejeitadas++
		return false
	}
	b.tokens -= n
	b.aceitas++
	return true
}

// DefinirTaxa altera a reposição sem renovar a capacidade de rajada.
func (b *Bucket) DefinirTaxa(taxa float64) error {
	if taxa <= 0 || math.IsNaN(taxa) || math.IsInf(taxa, 0) {
		return errors.New("taxa inválida")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	agora := time.Now()
	if !b.ultimo.IsZero() && agora.After(b.ultimo) {
		b.tokens = math.Min(b.capacidade, b.tokens+agora.Sub(b.ultimo).Seconds()*b.taxa)
	}
	b.ultimo = agora
	b.taxa = taxa
	return nil
}

func (b *Bucket) Contadores() (aceitas, rejeitadas uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.aceitas, b.rejeitadas
}
