# Middleware Pub/Sub em Go

Projeto de Programação Distribuída, cenário C de interação ao vivo.

Esta cópia contém a biblioteca cliente e o protocolo do repositório original, mais a implementação da parte 3 (controle de carga e avaliação). Consulte [PARTE3.md](PARTE3.md) para integração, comandos de demonstração, experimentos e interpretação das métricas.

## Componentes

- `internal/protocol`: protocolo binário sobre WebSocket.
- `internal/client`: biblioteca cliente Go com reconexão e inscrições.
- `internal/controle`: token bucket por sessão e resposta de rejeição.
- `internal/avaliacao`: gerador de carga, coleta de métricas e relatórios.
- `cmd/demo`: aplicação de demonstração no terminal.
- `cmd/carga`: ferramenta de avaliação.
- `cmd/fakebroker`: servidor de eco para testes manuais básicos da conexão. Não é o broker Pub/Sub.

## Verificação

Com o Go exigido pelo `go.mod`, execute na raiz:

```sh
go test ./...
go vet ./...
go build ./...
go run ./cmd/carga -h
go run ./cmd/demo -h
```

Os testes de avaliação sobem um broker mínimo interno de teste e não precisam de um broker externo. Para executar os comandos de carga e demonstração, a pessoa responsável pelo núcleo deve fornecer o broker real e integrar as respostas de controle descritas em `PARTE3.md`.

## Origem desta entrega

Base: `github.com/Luizalima3/middleware-pubsub-go`, commit `b184062`. As mudanças desta cópia foram preparadas localmente e não publicadas no GitHub. A entrega não inclui resultados de desempenho do broker de produção nem implementação da coordenação entre brokers.
