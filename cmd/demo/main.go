// demo é uma aplicação de interação ao vivo no terminal usando a biblioteca Go.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/client"
	"github.com/Luizalima3/middleware-pubsub-go/internal/protocol"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	brokers := flag.String("brokers", "ws://localhost:8080/ws", "URLs separadas por vírgula")
	modo := flag.String("modo", "assistir", "criar, assistir ou publicar")
	topico := flag.String("topico", "evento/reacoes", "tópico do evento")
	texto := flag.String("mensagem", "Boa jogada!", "reação a publicar")
	intervalo := flag.Duration("intervalo", time.Second, "intervalo entre reações")
	lento := flag.Duration("atraso", 0, "atraso de consumo do espectador")
	flag.Parse()
	if *modo != "criar" && *modo != "assistir" && *modo != "publicar" {
		return fmt.Errorf("modo inválido: %s", *modo)
	}
	if *intervalo <= 0 || *lento < 0 {
		return fmt.Errorf("intervalo deve ser positivo e atraso não negativo")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	dialCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	c, err := client.Conectar(dialCtx, strings.Split(*brokers, ","))
	stop()
	if err != nil {
		return err
	}
	defer c.Fechar()
	enviar := func(f func(context.Context) (uint32, error)) (uint32, error) {
		sctx, done := context.WithTimeout(ctx, 5*time.Second)
		defer done()
		return f(sctx)
	}
	if *modo == "criar" {
		id, err := enviar(func(x context.Context) (uint32, error) { return c.CriarTopico(x, *topico) })
		if err != nil {
			return err
		}
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return fmt.Errorf("broker não confirmou a criação")
			case m, ok := <-c.Eventos():
				if !ok {
					return fmt.Errorf("cliente encerrado")
				}
				if m.ID == id {
					if m.Tipo == protocol.TipoOK {
						fmt.Println("Tópico criado:", *topico)
						return nil
					}
					if m.Tipo == protocol.TipoErro {
						return fmt.Errorf("broker: %s", m.Conteudo)
					}
				}
			}
		}
	}
	if *modo == "assistir" {
		if _, err = enviar(func(x context.Context) (uint32, error) { return c.Assinar(x, *topico) }); err != nil {
			return err
		}
	}
	fmt.Printf("Modo %s | tópico %s | Ctrl+C encerra\n", *modo, *topico)
	ticker := time.NewTicker(*intervalo)
	defer ticker.Stop()
	n := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-c.Eventos():
			if !ok {
				return nil
			}
			if m.Tipo == protocol.TipoMensagem && *lento > 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(*lento):
				}
			}
			fmt.Printf("%s tipo=%d id=%d tópico=%s conteúdo=%s\n", time.Now().Format("15:04:05.000"), m.Tipo, m.ID, m.Topico, m.Conteudo)
		case <-ticker.C:
			if *modo != "publicar" {
				continue
			}
			n++
			_, err = enviar(func(x context.Context) (uint32, error) {
				return c.Publicar(x, *topico, []byte(fmt.Sprintf("Reação %d: %s", n, *texto)))
			})
			if err != nil {
				fmt.Println("Falha ao publicar:", err)
			}
		}
	}
}
