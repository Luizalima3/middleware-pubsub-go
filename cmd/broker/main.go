// broker executa o servidor UDP. O par é opcional; o projeto usa até dois brokers.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Luizalima3/middleware-pubsub-go/internal/broker"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	var c broker.Config
	flag.StringVar(&c.Endereco, "endereco", "127.0.0.1:8080", "endereço UDP local")
	flag.StringVar(&c.Par, "par", "", "endereço UDP do outro broker")
	flag.StringVar(&c.ID, "id", "", "identificador do broker (opcional)")
	flag.Float64Var(&c.TaxaCliente, "teto-cliente", 100, "publicações/s por identificador e teto de entrega por consumidor")
	flag.Float64Var(&c.TaxaEntrada, "teto-entrada", 2000, "publicações/s aceitas no total, incluindo repasses")
	flag.Float64Var(&c.TaxaSaida, "teto-saida", 5000, "datagramas/s totais de saída, incluindo controle")
	flag.Float64Var(&c.TaxaControle, "teto-controle", 200, "datagramas/s de controle por sentido")
	flag.Float64Var(&c.TaxaPar, "teto-par", 1000, "repasses/s entre brokers por sentido")
	flag.IntVar(&c.Rajada, "rajada", 4, "fichas iniciais e limite de rajada por orçamento")
	flag.IntVar(&c.FilaTopico, "fila-topico", 128, "mensagens por tópico")
	flag.IntVar(&c.FilaCliente, "fila-cliente", 64, "mensagens por consumidor")
	flag.IntVar(&c.BytesTopico, "bytes-topico", 128000, "bytes máximos na fila de cada tópico")
	flag.IntVar(&c.BytesCliente, "bytes-cliente", 64000, "bytes máximos na fila de cada consumidor")
	flag.IntVar(&c.MaxClientes, "max-clientes", 256, "sessões UDP simultâneas")
	flag.IntVar(&c.MaxTopicos, "max-topicos", 128, "tópicos em memória")
	flag.DurationVar(&c.Intervalo, "intervalo", time.Second, "período de anúncios e manutenção")
	flag.DurationVar(&c.Inatividade, "inatividade", 5*time.Second, "expiração de clientes e interesses remotos")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	b, e := broker.Iniciar(c)
	if e != nil {
		return e
	}
	defer b.Fechar()
	fmt.Println(b.String())
	if c.Par != "" {
		fmt.Println("Par:", c.Par)
	}
	<-ctx.Done()
	fmt.Printf("Contadores finais: %+v\n", b.Estatisticas())
	return nil
}
