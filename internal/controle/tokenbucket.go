// Package controle limita publicações sem bloquear os demais clientes.
package controle

import (
	"errors"
	"math"
	"sync"
	"time"
)

// Bucket deve ser criado uma vez por sessão de publicador no broker.
// Um token corresponde a uma publicação; o tamanho máximo do frame continua
// sendo validado pelo protocolo. Novas sessões começam com a rajada completa.
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
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ultimo.IsZero() {
		b.ultimo = agora
	}
	if agora.After(b.ultimo) {
		b.tokens = math.Min(b.capacidade, b.tokens+agora.Sub(b.ultimo).Seconds()*b.taxa)
		b.ultimo = agora
	}
	if b.tokens < 1 {
		b.rejeitadas++
		return false
	}
	b.tokens--
	b.aceitas++
	return true
}

func (b *Bucket) Contadores() (aceitas, rejeitadas uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.aceitas, b.rejeitadas
}
