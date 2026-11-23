// Cliente de teste: usa a biblioteca pública client.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/client"
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	brokers := flag.String("brokers", "127.0.0.1:8080", "um ou dois endereços UDP, separados por vírgula")
	topico := flag.String("topico", "leilao/lance", "tópico do teste")
	texto := flag.String("mensagem", "100", "conteúdo da publicação")
	idCliente := flag.String("id", "", "identificador do cliente; vazio gera um ID")
	flag.Parse()

	ctx, cancelar := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelar()

	c, err := client.ConectarComConfig(ctx, client.Config{
		Brokers: strings.Split(*brokers, ","),
		ID:      *idCliente,
	})
	if err != nil {
		return err
	}
	defer c.Fechar()
	log.Println("conectado ao broker")

	id, err := c.Assinar(ctx, *topico)
	if err != nil {
		return err
	}
	log.Printf("enviei Assinar com id %d", id)

	id, err = c.Publicar(ctx, *topico, []byte(*texto))
	if err != nil {
		return err
	}
	log.Printf("enviei Publicar com id %d", id)

	// Aguarda a publicação, ignorando a confirmação administrativa já recebida.
	for {
		select {
		case m, ok := <-c.Eventos():
			if !ok {
				return errors.New("conexão encerrada")
			}
			if m.Tipo != protocol.TipoMensagem || m.Topico != *topico {
				log.Printf("evento: tipo=%s tópico=%s conteúdo=%s", m.Tipo, m.Topico, m.Conteudo)
				continue
			}
			log.Printf("recebi: tipo=%s id=%d topico=%s conteudo=%s",
				m.Tipo, m.ID, m.Topico, m.Conteudo)
			return nil
		case <-ctx.Done():
			return fmt.Errorf("tempo esgotado esperando o broker: %w", ctx.Err())
		}
	}
}
