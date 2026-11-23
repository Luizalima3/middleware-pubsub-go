// Teste de reconexão: publica a cada segundo até apertar Ctrl+C.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/client"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	brokers := flag.String("brokers", "127.0.0.1:8080", "um ou dois endereços UDP, separados por vírgula")
	topico := flag.String("topico", "leilao/lance", "tópico usado no teste")
	intervalo := flag.Duration("intervalo", time.Second, "intervalo entre publicações")
	flag.Parse()
	if *intervalo <= 0 {
		return fmt.Errorf("intervalo deve ser positivo")
	}

	ctx, cancelar := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancelar()

	c, err := client.Conectar(ctx, strings.Split(*brokers, ","))
	if err != nil {
		return err
	}
	defer c.Fechar()
	ultimoBroker := c.BrokerAtual()
	log.Println("conectado ao broker", ultimoBroker)

	if _, err := c.Assinar(ctx, *topico); err != nil {
		return err
	}
	log.Println("inscrição confirmada em", *topico)

	go func() {
		for m := range c.Eventos() {
			log.Printf("recebi: tipo=%s id=%d topico=%s conteudo=%s",
				m.Tipo, m.ID, m.Topico, m.Conteudo)
		}
	}()

	n := 0
	ticker := time.NewTicker(*intervalo)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			brokerAtual := c.BrokerAtual()
			if brokerAtual != ultimoBroker {
				log.Printf("broker ativo: %q", brokerAtual)
				ultimoBroker = brokerAtual
			}
			n++
			if _, err := c.Publicar(ctx, *topico, []byte(fmt.Sprint(n))); err != nil {
				log.Println("publicar:", err)
			}
		}
	}
}
