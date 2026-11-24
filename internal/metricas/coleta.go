package metricas

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/fila"
)

type Fila struct {
	Tipo, Nome string
	fila.Estado
	EmEnvio int
}

type Coleta struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// Iniciar não sobrescreve um experimento anterior. Flush por amostra preserva
// amostras completas já escritas mesmo quando o processo é terminado à força.
func Iniciar(ctx context.Context, arquivo string, intervalo time.Duration, filas func() []Fila) (*Coleta, error) {
	if intervalo < 10*time.Millisecond || filas == nil {
		return nil, errors.New("intervalo de métricas deve ser >= 10ms e snapshot obrigatório")
	}
	anterior, err := LerProcesso()
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Dir(arquivo), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(arquivo, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(f)
	header := []string{"amostra", "instante", "pid", "cpu_total_s", "intervalo_s", "cpu_pct_1_nucleo", "cpu_pct_maquina", "cpus_logicas", "ram_residente_bytes", "goroutines", "fila_tipo", "fila_nome", "itens", "bytes", "limite_itens", "limite_bytes", "pico_itens", "pico_bytes", "em_envio"}
	if err = w.Write(header); err == nil {
		w.Flush()
		err = w.Error()
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	c := &Coleta{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		defer func() {
			if err := f.Close(); c.err == nil {
				c.err = err
			}
		}()
		prevTime := time.Now()
		seq := 0
		escrever := func(inicial bool) error {
			p, e := LerProcesso()
			if e != nil {
				return e
			}
			now := time.Now()
			dt := now.Sub(prevTime).Seconds()
			cpu, normalizado := "", ""
			if !inicial {
				v := percentualCPU(anterior, p, dt)
				cpu = fmt.Sprintf("%.6f", v)
				normalizado = fmt.Sprintf("%.6f", v/float64(runtime.NumCPU()))
			} else {
				dt = 0
			}
			base := []string{strconv.Itoa(seq), now.Format(time.RFC3339Nano), strconv.Itoa(os.Getpid()), fmt.Sprintf("%.9f", p.CPUSegundos), fmt.Sprintf("%.9f", dt), cpu, normalizado, strconv.Itoa(runtime.NumCPU()), strconv.FormatUint(p.RAMBytes, 10), strconv.Itoa(runtime.NumGoroutine())}
			for _, q := range filas() {
				bytes, limiteBytes, picoItens, picoBytes := strconv.Itoa(q.Bytes), strconv.Itoa(q.LimiteBytes), strconv.Itoa(q.PicoItens), strconv.Itoa(q.PicoBytes)
				if q.Tipo == "controle" {
					bytes = ""
					limiteBytes = ""
					picoItens = ""
					picoBytes = ""
				}
				row := append(append([]string(nil), base...), q.Tipo, q.Nome, strconv.Itoa(q.Itens), bytes, strconv.Itoa(q.LimiteItens), limiteBytes, picoItens, picoBytes, strconv.Itoa(q.EmEnvio))
				if e = w.Write(row); e != nil {
					return e
				}
			}
			w.Flush()
			anterior = p
			prevTime = now
			seq++
			return w.Error()
		}
		if c.err = escrever(true); c.err != nil {
			return
		}
		ticker := time.NewTicker(intervalo)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				c.err = escrever(false)
				return
			case <-ticker.C:
				if c.err = escrever(false); c.err != nil {
					return
				}
			}
		}
	}()
	return c, nil
}

// Fechar aguarda o último flush; pode ser chamado mais de uma vez.
func (c *Coleta) Fechar() error { c.cancel(); <-c.done; return c.err }

// Concluida permite ao executável perceber falhas de disco durante a coleta.
func (c *Coleta) Concluida() <-chan struct{} { return c.done }
