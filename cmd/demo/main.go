// demo cria tópicos, publica reações ou acompanha mensagens no terminal.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Luizalima3/middleware-pubsub-go/client"
	"github.com/Luizalima3/middleware-pubsub-go/protocol"
	"os"
	"os/signal"
	"strings"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	brokers := flag.String("brokers", "127.0.0.1:8080", "um ou dois endereços UDP, separados por vírgula")
	modo := flag.String("modo", "assistir", "criar, apagar, assistir, cancelar, publicar, estatisticas ou interativo")
	topico := flag.String("topico", "evento/reacoes", "tópico")
	texto := flag.String("mensagem", "Boa jogada!", "texto da reação")
	intervalo := flag.Duration("intervalo", time.Second, "intervalo desejado entre reações")
	atraso := flag.Duration("atraso", 0, "atraso do consumidor")
	teto := flag.Float64("teto", 100, "teto local de publicações/s")
	id := flag.String("id", "", "identificador do cliente; vazio gera um ID")
	buffer := flag.Int("buffer", 32, "capacidade da fila de leitura do cliente")
	flag.Parse()
	if *intervalo <= 0 || *atraso < 0 {
		return fmt.Errorf("intervalos inválidos")
	}
	switch *modo {
	case "criar", "apagar", "assistir", "cancelar", "publicar", "estatisticas", "interativo":
	default:
		return fmt.Errorf("modo inválido")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	dial, done := context.WithTimeout(ctx, 5*time.Second)
	c, e := client.ConectarComConfig(dial, client.Config{Brokers: strings.Split(*brokers, ","), ID: *id, TaxaMaxima: *teto, Buffer: *buffer})
	done()
	if e != nil {
		return e
	}
	defer c.Fechar()
	if *modo == "interativo" {
		return interativo(ctx, c)
	}
	op, end := context.WithTimeout(ctx, 5*time.Second)
	defer end()
	switch *modo {
	case "criar":
		_, e = c.CriarTopico(op, *topico)
		if e == nil {
			fmt.Println("Tópico criado:", *topico)
		}
		return e
	case "apagar":
		_, e = c.ApagarTopico(op, *topico)
		if e == nil {
			fmt.Println("Tópico apagado:", *topico)
		}
		return e
	case "cancelar":
		_, e = c.Cancelar(op, *topico)
		return e
	case "estatisticas":
		m, e := c.Estatisticas(op)
		if e != nil {
			return e
		}
		var v any
		_ = json.Unmarshal(m.Conteudo, &v)
		d, _ := json.MarshalIndent(v, "", "  ")
		fmt.Println(string(d))
		return nil
	case "assistir":
		if _, e = c.Assinar(op, *topico); e != nil {
			return e
		}
	}
	fmt.Printf("Modo %s | tópico %s | broker %s | Ctrl+C encerra\n", *modo, *topico, c.BrokerAtual())
	// Um escritor separado permite continuar mostrando avisos enquanto a taxa espera.
	if *modo == "publicar" {
		go func() {
			t := time.NewTicker(*intervalo)
			defer t.Stop()
			n := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					n++
					x, cancel := context.WithTimeout(ctx, 2*time.Second)
					_, err := c.Publicar(x, *topico, []byte(fmt.Sprintf("Reação %d: %s", n, *texto)))
					cancel()
					if err != nil && ctx.Err() == nil {
						fmt.Fprintln(os.Stderr, "Envio:", err)
					}
				}
			}
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-c.Eventos():
			if !ok {
				return nil
			}
			if m.Tipo == protocol.TipoMensagem && *atraso > 0 {
				t := time.NewTimer(*atraso)
				select {
				case <-ctx.Done():
					t.Stop()
					return nil
				case <-t.C:
				}
			}
			fmt.Printf("%s tipo=%s id=%d tópico=%s conteúdo=%s taxa=%.1f/s\n", time.Now().Format("15:04:05.000"), m.Tipo, m.ID, m.Topico, m.Conteudo, c.Taxa())
		}
	}
}

// O mesmo cliente pode assinar e cancelar sem trocar de endpoint UDP.
func interativo(ctx context.Context, c *client.Client) error {
	linhas := make(chan string)
	fim := make(chan struct{})
	defer close(fim)
	go func() {
		defer close(linhas)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 2048), 4096)
		for scanner.Scan() {
			select {
			case linhas <- scanner.Text():
			case <-fim:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	fmt.Println("Comandos: criar TOPICO | apagar TOPICO | assinar TOPICO | cancelar TOPICO | publicar TOPICO TEXTO | sair")
	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-c.Eventos():
			if !ok {
				return nil
			}
			fmt.Printf("Recebido: %s tópico=%s conteúdo=%s\n", m.Tipo, m.Topico, m.Conteudo)
		case linha, ok := <-linhas:
			if !ok {
				return nil
			}
			p := strings.Fields(linha)
			if len(p) == 0 {
				continue
			}
			if p[0] == "sair" {
				return nil
			}
			if len(p) < 2 {
				fmt.Println("Informe o tópico.")
				continue
			}
			x, cancel := context.WithTimeout(ctx, 3*time.Second)
			var e error
			switch p[0] {
			case "criar":
				_, e = c.CriarTopico(x, p[1])
			case "apagar":
				_, e = c.ApagarTopico(x, p[1])
			case "assinar":
				_, e = c.Assinar(x, p[1])
			case "cancelar":
				_, e = c.Cancelar(x, p[1])
			case "publicar":
				if len(p) < 3 {
					e = fmt.Errorf("informe o texto")
				} else {
					_, e = c.Publicar(x, p[1], []byte(strings.Join(p[2:], " ")))
				}
			default:
				e = fmt.Errorf("comando desconhecido")
			}
			cancel()
			if e != nil {
				fmt.Println("Erro:", e)
			} else if p[0] == "publicar" {
				fmt.Println("Datagrama enviado; entrega não confirmada.")
			} else {
				fmt.Println("Comando confirmado.")
			}
		}
	}
}
