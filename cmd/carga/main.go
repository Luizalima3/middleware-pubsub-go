// carga gera publicações em agenda fixa e exporta JSON, CSV, HTML e SVG.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/avaliacao"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	var c avaliacao.Config
	var brokers, dir string
	flag.StringVar(&brokers, "brokers", "ws://localhost:8080/ws", "URLs separadas por vírgula")
	flag.StringVar(&c.Topico, "topico", "teste/carga", "tópico exclusivo do experimento")
	flag.BoolVar(&c.TopicoExistente, "topico-existente", false, "usar tópico já criado, sem solicitar criação")
	flag.IntVar(&c.Publicadores, "publicadores", 1, "número de publicadores")
	flag.IntVar(&c.Assinantes, "assinantes", 10, "número de assinantes")
	flag.Float64Var(&c.Taxa, "taxa", 100, "mensagens/s por publicador")
	flag.DurationVar(&c.Duracao, "duracao", 10*time.Second, "janela de publicação")
	flag.DurationVar(&c.Drenagem, "drenagem", 2*time.Second, "espera pelas entregas ao final")
	flag.DurationVar(&c.Timeout, "timeout", 5*time.Second, "limite para conexão, ACK ou escrita")
	flag.IntVar(&c.Payload, "payload", 128, "bytes do payload, mínimo 32")
	flag.IntVar(&c.Fila, "fila", 64, "capacidade da fila de envio do gerador por publicador")
	flag.IntVar(&c.Lentos, "lentos", 0, "quantos assinantes consomem com atraso")
	flag.DurationVar(&c.AtrasoLento, "atraso-lento", 100*time.Millisecond, "atraso por mensagem nos assinantes lentos")
	flag.StringVar(&dir, "saida", "resultados/carga", "pasta de saída; use uma pasta por execução")
	flag.Parse()
	for _, s := range strings.Split(brokers, ",") {
		if s = strings.TrimSpace(s); s != "" {
			c.URLs = append(c.URLs, s)
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	fmt.Println("Preparando clientes: o broker precisa confirmar criação e inscrições com TipoOK.")
	r, err := avaliacao.Executar(ctx, c)
	if err != nil {
		return err
	}
	if err = avaliacao.Salvar(dir, r); err != nil {
		return err
	}
	fmt.Printf("Escritas: %d | entregas únicas: %d | ausentes: %d | puladas: %d | p99: %.3f ms\nRelatório: %s/relatorio.html\n", r.Resumo.Enviadas, r.Resumo.Recebidas, r.Resumo.Ausentes, r.Resumo.Puladas, r.Resumo.P99MS, dir)
	return nil
}
