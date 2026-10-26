// Cliente de teste: usa a biblioteca internal/client.
package main

import (
	"context"
	"log"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/client"
)

func main() {
	ctx, cancelar := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelar()

	c, err := client.Conectar(ctx, "ws://localhost:8080/ws")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Fechar()
	log.Println("conectado ao broker")

	id, err := c.Assinar(ctx, "leilao/lance")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enviei Assinar com id %d", id)

	id, err = c.Publicar(ctx, "leilao/lance", []byte("100"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enviei Publicar com id %d", id)

	// O broker falso ecoa tudo, então esperamos receber os dois frames de volta.
	for i := 0; i < 2; i++ {
		select {
		case m, ok := <-c.Eventos():
			if !ok {
				log.Fatal("conexão encerrada")
			}
			log.Printf("recebi: tipo=%d id=%d topico=%s conteudo=%s",
				m.Tipo, m.ID, m.Topico, m.Conteudo)
		case <-ctx.Done():
			log.Fatal("tempo esgotado esperando o broker")
		}
	}
}
