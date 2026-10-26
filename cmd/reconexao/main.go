// Teste de reconexão: publica a cada segundo até apertar Ctrl+C.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/client"
)

func main() {
	ctx, cancelar := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancelar()

	c, err := client.Conectar(ctx, []string{"ws://localhost:8080/ws"})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Fechar()
	log.Println("conectado ao broker")

	if _, err := c.Assinar(ctx, "leilao/lance"); err != nil {
		log.Fatal(err)
	}

	go func() {
		for m := range c.Eventos() {
			log.Printf("recebi: tipo=%d id=%d topico=%s conteudo=%s",
				m.Tipo, m.ID, m.Topico, m.Conteudo)
		}
	}()

	n := 0
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n++
			if _, err := c.Publicar(ctx, "leilao/lance", []byte(fmt.Sprint(n))); err != nil {
				log.Println("publicar:", err)
			}
		}
	}
}
