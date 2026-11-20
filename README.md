# Middleware Pub/Sub - Cenário C

Implementação acadêmica em Go: **UDP + JSON, clientes no terminal, taxa adaptativa com teto e até dois brokers**. Baseada no documento simplificado v3 de 05/11/2026.

## Requisitos

- Go 1.23 ou superior. Sem bibliotecas externas.
- Para os exemplos, executar os comandos na raiz do repositório.
- Ambiente previsto: rede local controlada, endereços conhecidos e mensagens de até 1.000 bytes completos.
- Não há página web. O HTML gerado é apenas um relatório local de resultados.

## Começar com um broker

Terminal 1:

```sh
go run ./cmd/broker -endereco 127.0.0.1:8080
```

Terminal 2, espectador:

```sh
go run ./cmd/demo -modo assistir -topico jogo/reacoes
```

Terminal 3, participante:

```sh
go run ./cmd/demo -modo publicar -topico jogo/reacoes -mensagem "Gol!" -intervalo 100ms
```

ASSINAR cria um tópico inexistente. Também é possível criá-lo e apagá-lo explicitamente:

```sh
go run ./cmd/demo -modo criar -topico jogo/reacoes
go run ./cmd/demo -modo apagar -topico jogo/reacoes
go run ./cmd/demo -modo estatisticas
```

Uma publicação em tópico inexistente gera TOPICO_INEXISTENTE. Se o tópico existe e não há inscritos conhecidos, o publicador recebe SEM_INSCRITOS.

## Cliente interativo

Para criar, assinar, publicar e cancelar no mesmo cliente:

```sh
go run ./cmd/demo -modo interativo
```

Digite um comando por linha:

```text
criar jogo/reacoes
assinar jogo/reacoes
publicar jogo/reacoes Gol!
cancelar jogo/reacoes
apagar jogo/reacoes
sair
```

Espere a mensagem recebida antes de cancelar se quiser observar a entrega. O comando cancelar de uma instância não cancela a inscrição de outro endpoint.

## Dois brokers

Use um terminal para cada comando:

```sh
go run ./cmd/broker -endereco 127.0.0.1:8080 -par 127.0.0.1:8081 -id b1
go run ./cmd/broker -endereco 127.0.0.1:8081 -par 127.0.0.1:8080 -id b2
```

Conecte o espectador ao primeiro e o publicador ao segundo:

```sh
go run ./cmd/demo -modo assistir -brokers 127.0.0.1:8080,127.0.0.1:8081 -topico jogo/reacoes
go run ./cmd/demo -modo publicar -brokers 127.0.0.1:8081,127.0.0.1:8080 -topico jogo/reacoes
```

O interesse pelos tópicos é anunciado periodicamente; aguarde a propagação inicial antes de medir. Ao encerrar uma instância, a biblioteca tenta o outro endereço e reenvia as inscrições. Há interrupção e podem ocorrer perdas. Para simular uma falha real, encerre também o processo broker que estiver sendo usado; compilar antes torna isso mais fácil.

## Controle de tráfego

- O cliente começa no seu teto local, limitado pelo teto informado pelo broker.
- Pressão na fila ou sinais repetidos de perda reduzem a taxa pela metade.
- Após duas avaliações boas **e com demanda**, uma taxa reduzida recupera 10%, até o teto.
- Situação estável no teto não causa redução automática.
- Sem feedback recente, a taxa diminui e a biblioteca verifica a disponibilidade.
- As fichas limitam rajadas; alterar a taxa não renova o balde.

O broker aplica limites por ID de cliente e limites globais de entrada e saída. O teto global de saída inclui dados, controle e repasses; controle e repasses possuem também orçamentos próprios. Os tetos por fluxo são conservadores e repartidos entre participantes ativos. Não há promessa de justiça perfeita nem controle de congestionamento validado para a Internet.

Os tetos são taxas sustentadas; a rajada configurada pode permitir envios concentrados no início de uma janela. Como cada datagrama tem até 1.000 bytes, um teto em datagramas/s também estabelece um limite superior de bytes/s, acrescido da rajada e dos cabeçalhos UDP/IP. Filas são limitadas por itens e por bytes.

```sh
go run ./cmd/broker -teto-cliente 100 -teto-entrada 1000 -teto-saida 3000 -rajada 4
go run ./cmd/demo -modo assistir -atraso 100ms -buffer 4 -topico jogo/reacoes
```

## Avaliação

```sh
go run ./cmd/carga -topico teste/normal -publicadores 2 -assinantes 10 -taxa 30 -duracao 10s -saida resultados/normal
go run ./cmd/carga -topico teste/lento -publicadores 2 -assinantes 10 -lentos 1 -atraso-lento 100ms -buffer-cliente 4 -taxa 30 -duracao 15s -saida resultados/lento
go run ./cmd/carga -topico teste/agressivo -publicadores 1 -assinantes 10 -taxa 1000 -ignorar-controle -duracao 10s -saida resultados/agressivo
go run ./cmd/carga -brokers 127.0.0.1:8080,127.0.0.1:8081 -distribuir -topico teste/dois -duracao 10s -saida resultados/dois
```

O cenário agressivo ignora voluntariamente o controle **no cliente** para verificar a proteção do broker. Para avaliar impacto sobre usuários normais, execute um segundo gerador no mesmo tópico com taxa normal, usando -topico-existente e uma saída diferente.

Cada execução gera resumo.json, intervalos.csv, vazao.svg, latencia.svg, taxa.svg e relatorio.html. [Como medir e interpretar](PARTE3.md).

## Verificação

```sh
go test ./...
go vet ./...
go build ./...
```

Os testes usam brokers UDP reais locais. Incluem operações, repasse em um salto, falha com reinscrição, consumidor lento, tetos, aviso repetido e confirmação perdida.

## Organização

- client/: biblioteca pública importável por outros programas Go.
- protocol/: formato público JSON e validação.
- internal/broker/: servidor, filas, controle de clientes e repasse.
- internal/controle/: token bucket e regra de ajuste.
- internal/fila/: fila limitada por quantidade e bytes.
- internal/recente/: IDs recentes com limite e expiração.
- internal/avaliacao/: carga, métricas e relatórios.
- cmd/broker, cmd/demo, cmd/carga: executáveis principais.

[Núcleo do broker](BROKER.md) · [Protocolo e comportamento](PROTOCOL.md) · [Biblioteca cliente](client/README.md) · [Correspondência com o documento](ADEQUACAO.md).

## Limitações

UDP pode perder, duplicar e reordenar mensagens. O sucesso de Publicar confirma apenas a escrita UDP local. Publicações não são retransmitidas. Comandos e avisos importantes têm confirmação e tentativas limitadas; se todas falharem, o resultado pode ser desconhecido. Memória recente para duplicatas expira e não constitui garantia permanente de execução única.

A rede de brokers tem apenas duas instâncias, sem consenso. Uma partição pode deixar interesses e tópicos temporariamente diferentes. A administração de tópicos deve ser sequencial na demonstração. O ID de cliente é declarado, sem autenticação: pressupõe-se um ambiente acadêmico confiável.

A interpretação de encaminhamento como tentativa de envio, com perdas e descarte na sobrecarga, precisa estar alinhada com o professor. Não há resultados de capacidade de produção nem garantia de recuperação sem interrupção.
