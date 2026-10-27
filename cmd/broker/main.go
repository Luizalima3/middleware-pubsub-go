// broker é o executável do núcleo Publish/Subscribe (Cenário C).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/broker"
)

func main() {
	if err := executar(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func executar() error {
	var (
		addr           string
		filaAssinante  int
		filaControle   int
		filaTopico     int
		taxa           float64
		rajada         int
		timeoutEscrita time.Duration
		origens        string
	)

	flag.StringVar(&addr, "addr", ":8080", "endereço HTTP de escuta (rotas /ws e /contadores)")
	flag.IntVar(&filaAssinante, "fila-assinante", broker.FilaAssinantePadrao, "capacidade da fila de dados por inscrito (drop-oldest)")
	flag.IntVar(&filaControle, "fila-controle", broker.FilaControlePadrao, "capacidade da fila de controle por sessão")
	flag.IntVar(&filaTopico, "fila-topico", broker.FilaTopicoPadrao, "capacidade da fila de cada tópico")
	flag.Float64Var(&taxa, "taxa", broker.TaxaPadrao, "publicações/s permitidas por sessão (token bucket)")
	flag.IntVar(&rajada, "rajada", broker.RajadaPadrao, "rajada máxima de publicações por sessão (token bucket)")
	flag.DurationVar(&timeoutEscrita, "timeout-escrita", broker.TimeoutEscritaPadrao, "tempo limite de cada escrita no socket")
	flag.StringVar(&origens, "origens", "*", "padrões de Origin aceitos no WebSocket, separados por vírgula")
	flag.Parse()

	if filaAssinante < 1 || filaControle < 1 || filaTopico < 1 {
		return errors.New("as capacidades das filas devem ser >= 1")
	}
	if taxa <= 0 || rajada < 1 {
		return errors.New("taxa deve ser > 0 e rajada >= 1")
	}
	if timeoutEscrita <= 0 {
		return errors.New("timeout-escrita deve ser > 0")
	}

	var padroesOrigem []string
	for _, s := range strings.Split(origens, ",") {
		if s = strings.TrimSpace(s); s != "" {
			padroesOrigem = append(padroesOrigem, s)
		}
	}

	b := broker.Novo(broker.Config{
		FilaAssinante:  filaAssinante,
		FilaControle:   filaControle,
		FilaTopico:     filaTopico,
		Taxa:           taxa,
		Rajada:         rajada,
		TimeoutEscrita: timeoutEscrita,
		Origens:        padroesOrigem,
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("escutar em %s: %w", addr, err)
	}

	srv := &http.Server{
		Handler:           b.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, parar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer parar()

	erroSrv := make(chan error, 1)
	go func() {
		log.Printf("broker ouvindo em %s (ws://localhost%s/ws | http://localhost%s/contadores)", ln.Addr(), addr, addr)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			erroSrv <- err
			return
		}
		erroSrv <- nil
	}()

	select {
	case err := <-erroSrv:
		_ = b.Fechar()
		return err
	case <-ctx.Done():
		log.Println("sinal recebido: encerrando broker graciosamente...")
		_ = b.Fechar()
		ctxShutdown, cancelar := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelar()
		_ = srv.Shutdown(ctxShutdown)
		return <-erroSrv
	}
}
