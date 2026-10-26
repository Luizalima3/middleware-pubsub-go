# Parte 3 Controle de carga e avaliação

Esta entrega implementa o controle de taxa, uma demonstração de interação ao vivo no terminal e um gerador de carga com relatórios. Usa WebSocket e o protocolo binário já existente. O broker de produção continua sendo a responsabilidade da pessoa 1; a biblioteca cliente continua sendo a responsabilidade da pessoa 2.

## O que está pronto

| Componente | Local | Função |
| --- | --- | --- |
| Token bucket concorrente | `internal/controle` | Limite por sessão, rajadas, rejeição imediata e contadores |
| Integração com protocolo | `Bucket.Verificar` | Rejeita publicações excedentes com `TipoErro` e conteúdo `RATE_LIMIT` |
| Gerador de carga | `cmd/carga` e `internal/avaliacao` | Agenda fixa, fila limitada, assinantes normais e lentos |
| Demonstração de evento | `cmd/demo` | Criar tópico, assistir e publicar reações pelo terminal |
| Resultados | `avaliacao.Salvar` | JSON, CSV, dois gráficos SVG e relatório HTML local |
| Testes | `internal/controle/*_test.go`, `internal/avaliacao/carga_test.go` | Concorrência, reposição, isolamento, fan-out, ausências, rejeições e exportação |

Os dois exemplos antigos também foram ajustados para a assinatura `Conectar(ctx, []string{...})`. Não há mudanças na semântica do protocolo nem na biblioteca cliente.

## Pré-requisitos

- Go na versão exigida pelo `go.mod` (1.27.3), ou toolchain automático autorizado a baixá-la.
- Executar os comandos a partir da raiz deste repositório.
- Para demonstração e carga: broker real iniciado e acessível em `ws://localhost:8080/ws`.

```sh
go test ./...
go vet ./...
```

`cmd/fakebroker` é um servidor de eco: não implementa inscrições e não serve para avaliar o Pub/Sub. O gerador detecta sua falta de confirmação durante a preparação e encerra com erro. Os testes usam um broker mínimo exclusivo dos arquivos de teste, sem tratá-lo como implementação final.

## Integração da pessoa 1

Crie um bucket **uma vez por conexão**, fora do laço de leitura. Não recrie o bucket a cada mensagem, pois isso restauraria a rajada. Exemplo de integração no manipulador de sessão:

```go
bucket, err := controle.Novo(100, 200) // 100 publicações/s; rajada de 200
if err != nil {
    return err
}

// Dentro do laço, após decodificar e validar a mensagem:
if aviso := bucket.Verificar(mensagem); aviso != nil {
    // Colocar o aviso na fila de saída do publicador.
    // A fila também precisa de limite; não escrever segurando o lock do broker.
    enfileirarResposta(*aviso)
    continue // Não encaminhar a publicação recusada.
}
processarNoNucleo(mensagem)
```

`enfileirarResposta` e `processarNoNucleo` representam as funções do broker a ser implementado; não são funções fornecidas neste pacote. O bucket não dorme e não cria fila de espera. A sincronização é independente por sessão.

`Contadores()` retorna publicações aceitas pelo limite e recusadas. “Aceita pelo limite” não significa entregue: a mensagem ainda pode ser descartada por ausência de inscritos ou filas cheias. Exponham esses contadores nas métricas do broker separadamente. Os valores 100/200 são exemplos configuráveis, não metas do enunciado.

Limitação: o limite é por conexão, não por identidade autenticada. Reconectar restaura a rajada, e abrir várias conexões multiplica a cota. Um limite por participante entre reconexões/brokers exige um identificador e política compartilhada acordados pela equipe. A validação do tamanho máximo dos frames continua necessária.

## Contrato necessário com as pessoas 1 e 2

| Operação ou evento | Resposta esperada |
| --- | --- |
| `TipoCriarTopico` | `TipoOK` com o mesmo ID após criação; `TipoErro` em caso de falha |
| `TipoAssinar` | `TipoOK` com o mesmo ID **depois** de efetivar a inscrição |
| Entrega ao inscrito | `TipoMensagem`, mesmo tópico e payload preservado |
| Publicação excedeu a cota | `TipoErro`, ID original, conteúdo UTF-8 exato `RATE_LIMIT` |
| Publicação sem inscritos | `TipoSemInscritos`, ID original |

Essas confirmações são de **controle**, não ACKs para cada publicação. Evitam iniciar o experimento antes de as inscrições existirem. O protocolo atual já define todos os tipos usados; o comportamento das respostas precisa ser integrado ao broker real. Para múltiplos brokers, o ACK deve ter a semântica de prontidão que a equipe definir. A configuração padrão conecta todos os clientes ao primeiro endereço disponível; informar várias URLs testa failover, não balanceamento automático.

Use um tópico novo por experimento. Se a criação de tópico existente gerar erro, escolha outro nome. O gerador não exclui automaticamente o tópico ao terminar, para não apagar recursos que outra aplicação possa estar usando.

## Demonstração em três terminais

Depois de iniciar o broker real, crie o tópico uma vez:

```sh
go run ./cmd/demo -modo criar -topico evento/reacoes
```

Terminal 1, espectador normal:

```sh
go run ./cmd/demo -modo assistir -topico evento/reacoes
```

Terminal 2, espectador lento:

```sh
go run ./cmd/demo -modo assistir -topico evento/reacoes -atraso 200ms
```

Terminal 3, participante:

```sh
go run ./cmd/demo -modo publicar -topico evento/reacoes -intervalo 10ms -mensagem "Boa jogada!"
```

O terminal exibe as entregas e os avisos. Encerre com Ctrl+C. Trata-se de uma aplicação Go no terminal; não inclui cliente JavaScript nem página de demonstração no navegador.

## Experimentos reproduzíveis

Os comandos abaixo são exemplos de carga, não resultados obtidos. Rode cada cenário em pasta e tópico próprios, preserve os artefatos e repita as execuções sob condições equivalentes.

### Carga normal

```sh
go run ./cmd/carga -topico teste/normal01 -publicadores 2 -assinantes 20 -taxa 50 -duracao 30s -saida resultados/normal01
```

### Consumidor lento

```sh
go run ./cmd/carga -topico teste/lento01 -publicadores 2 -assinantes 20 -lentos 1 -atraso-lento 100ms -taxa 50 -duracao 30s -drenagem 5s -saida resultados/lento01
```

Compare o p99 dos assinantes normais com o cenário sem atraso. O atraso ocorre no consumo de `Eventos()`, cujo buffer atual tem 256 posições; um teste curto pode não fazer o socket do consumidor exercer pressão no broker. O descarte na fila do broker só pode ser demonstrado com sua instrumentação e carga suficiente.

### Publicador acima da cota

```sh
go run ./cmd/carga -topico teste/agressivo01 -publicadores 1 -assinantes 20 -taxa 1000 -duracao 30s -saida resultados/agressivo01
```

Só haverá rejeição por limite quando o broker realmente integrar o bucket. Para testar justiça, execute simultaneamente outra instância do gerador com taxa normal, no mesmo tópico e pasta de saída diferente. Na segunda instância use `-topico-existente`, depois de o tópico estar criado. Cada execução tem um identificador próprio e ignora payloads da outra ao calcular latência. Todas as mensagens continuam gerando carga no broker.

### Falha de broker

```sh
go run ./cmd/carga -brokers ws://localhost:8080/ws,ws://localhost:8081/ws -topico teste/falha01 -duracao 45s -saida resultados/falha01
```

Com os dois brokers reais coordenados, derrube a instância usada durante a execução. Observe os intervalos sem entregas e a recuperação. O gerador não implementa a coordenação entre brokers nem prova disponibilidade apenas por receber uma lista de endereços. O relatório ainda não calcula automaticamente a duração exata da indisponibilidade.

## Como interpretar as métricas

- **Agenda fixa:** cada publicador possui horários independentes de respostas. Uma fila limitada separa a agenda das escritas. Se o gerador não acompanhar a carga, registra publicações puladas em vez de aumentar a memória indefinidamente.
- **Publicações planejadas e não agendadas:** o JSON preserva o total previsto; se a execução terminar ou o agendador não alcançar todos os horários, a diferença fica em `publicacoes_nao_agendadas`. Entre as publicações oferecidas, `enviadas + erros_de_envio + puladas` deve fechar o total. O encerramento manual é marcado no resumo.
- **Latência:** do horário agendado ao consumo pela aplicação. Inclui o atraso do gerador, filas e atraso artificial do consumidor lento. Relógio monotônico do mesmo processo; o payload não usa relógios de máquinas distintas.
- **Percentis:** p50, p95 e p99 aproximados por histograma de memória fixa, com limite superior de buckets de 2%. O relatório também separa p99 dos consumidores normais e lentos e atraso p99 do gerador. Amostra vazia é exportada como zero; consulte o número de entregas.
- **Escritas sem erro:** a biblioteca conseguiu escrever no socket. Não confirmam que o broker aceitou a publicação.
- **Entregas ausentes:** pares publicação/assinante de escritas sem erro que não chegaram até o fim da drenagem. Incluem rejeições, descartes ou mensagens atrasadas; não são automaticamente perdas de rede. Envios que retornaram erro podem ter sido processados parcialmente e ficam fora desse denominador.
- **Vazão:** publicações por segundo da duração configurada; entregas por segundo da janela observada, que inclui drenagem. O CSV usa contagens por intervalos de um segundo, incluindo períodos sem mensagens.
- **Avisos:** rejeições de taxa e ausência de inscritos são contados quando recebidos; podem ser incompletos se o broker não conseguir enviar seus avisos.
- **Recursos:** CPU e memória do servidor precisam ser medidas no processo/container do broker. Estas ferramentas não atribuem recursos do gerador ao servidor.

Use `go run ./cmd/carga -h` para consultar todas as opções. O gerador limita a duração a 10 minutos, drenagem a 1 minuto, publicações planejadas a 1 milhão e pares de entrega a 100 milhões, para limitar suas próprias estruturas. Evite executar cargas pesadas no mesmo processo/máquina do broker ao comparar desempenho. Para avaliações repetidas, prefira compilar antes (`go build -o carga ./cmd/carga`) e executar o binário.

## Resultados produzidos

Cada execução bem-sucedida gera:

- `resumo.json`: configuração, contadores e percentis.
- `intervalos.csv`: evolução em intervalos de um segundo.
- `vazao.svg` e `latencia.svg`: gráficos vetoriais exportáveis.
- `relatorio.html`: relatório que pode ser aberto localmente no navegador, sem internet.

Não há metas inventadas de latência ou resultados de produção nesta entrega. A equipe deve acordar metas, registrar hardware, tamanho de mensagem, clientes, repetições e limitações, e então avaliar o broker real.
