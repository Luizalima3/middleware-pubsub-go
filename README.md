# Middleware Publish/Subscribe — Cenário C

Implementação acadêmica em Go de um middleware de distribuição de mensagens no padrão Publish/Subscribe. O objetivo é desacoplar publicadores e assinantes: o publicador envia uma mensagem para um tópico, e o middleware a encaminha aos clientes inscritos, sem que os participantes precisem conhecer uns aos outros.

O projeto usa datagramas UDP com mensagens JSON, tópicos e inscrições mantidos em memória, controle de tráfego e suporte a até dois brokers configurados previamente. É voltado a demonstrações e experimentos em uma rede controlada; não é um serviço de produção.

## Visão geral e arquitetura

```text
Publicador ── PUBLICAR ──> Broker ── MENSAGEM ──> Assinantes
                              │
                              ├── filas e controle de tráfego
                              │
                              └── INTERESSE / REPASSE ──> Broker par
```

Os clientes comunicam-se com um broker por UDP. O broker valida e interpreta cada datagrama, administra os tópicos e as inscrições e, quando aceita uma publicação, a coloca nas filas de distribuição. A comunicação entre brokers permite anunciar interesses remotos e encaminhar mensagens para o único par configurado.

| Componente | Responsabilidade |
| --- | --- |
| `protocol/` | Tipos de mensagem, codificação/decodificação JSON e validações do protocolo. |
| `client/` | Biblioteca Go para conexão UDP, operações, recebimento, deduplicação local, controle de taxa e failover do cliente. |
| `internal/broker/` | Servidor UDP, sessões de clientes, tópicos, inscrições, filas, distribuição, controle no broker e comunicação com o par. |
| `internal/controle/` | Token bucket e mecanismo adaptativo de taxa utilizado pelo cliente e pelo broker. |
| `internal/fila/` | Filas limitadas por quantidade de itens e por bytes. |
| `internal/recente/` | Cache em memória de identificadores recentes com capacidade e expiração limitadas. |
| `internal/avaliacao/` | Gerador de carga, coleta de resultados e geração de arquivos de avaliação. |
| `cmd/` | Executáveis do broker, gerador de carga e demonstrações do cliente. |

O broker mantém dados em memória. Não há banco de dados ou persistência entre reinicializações.

## Tecnologias e limites do protocolo

- Go 1.23 ou superior; o módulo não declara dependências externas.
- UDP, usando a biblioteca padrão `net`.
- JSON UTF-8, usando `encoding/json`; cada datagrama tem limite de 1.000 bytes.
- Tópicos de até 80 bytes e identificadores de cliente de até 64 bytes.
- Comunicação com endereços de broker conhecidos. O identificador do cliente é declarado no protocolo, não é uma credencial nem mecanismo de autenticação.

As mensagens JSON incluem um `tipo` e, conforme a operação, campos como `id`, `cliente`, `topico`, `conteudo` e dados de atividade. `conteudo` é texto UTF-8. A validação do protocolo rejeita datagramas acima do limite, JSON inválido, campos desconhecidos e tópicos inválidos.

## Operações e fluxo das mensagens

| Operação | Efeito |
| --- | --- |
| `CRIAR_TOPICO` | Cria um tópico no broker, se houver capacidade. |
| `APAGAR_TOPICO` | Remove o tópico e suas filas; os inscritos recebem um aviso. |
| `ASSINAR` | Cria o tópico se necessário e inscreve o endpoint UDP do cliente. |
| `CANCELAR` | Remove a inscrição daquele endpoint no tópico. |
| `PUBLICAR` | Solicita a publicação do conteúdo no tópico. |
| `ATIVIDADE` / `ESTADO` | Troca atividade, ocupação e contadores usados no controle de tráfego. |

As quatro operações administrativas de tópico retornam `OK` ou `ERRO`. O cliente repete pedidos administrativos com o mesmo ID e o mesmo broker por um número limitado de tentativas; o broker mantém respostas recentes para reconhecer repetições. `ESTATISTICAS` também pode ser solicitado pelo cliente.

`PUBLICAR` é de melhor esforço: sucesso na API do cliente significa que a escrita UDP local ocorreu, não que o broker aceitou ou entregou a mensagem. O broker informa rejeições por avisos; pode responder `ERRO` quando o tópico não existe ou a publicação é rejeitada, e `SEM_INSCRITOS` quando não há assinantes locais ou interesse remoto conhecido.

Avisos enviados pelo broker usam confirmações e repetição limitada. Publicações e respostas podem ser perdidas, duplicadas ou reordenadas, como é próprio de UDP.

## Brokers, tópicos, filas e repasse

Um broker aceita clientes UDP, mantém sessões associadas aos endpoints e guarda tópicos e inscrições em memória. As publicações aceitas percorrem uma fila do tópico e são distribuídas às filas dos consumidores. Os limites padrão são 128 itens/128.000 bytes por tópico e 64 itens/64.000 bytes por consumidor; são configuráveis. A fila do tópico recusa novas entradas quando cheia. Na fila do consumidor, mensagens antigas podem ser descartadas para abrir espaço; o broker contabiliza e informa descartes.

Um broker pode ter um único par configurado com `-par`. Os brokers anunciam periodicamente interesses em tópicos; o par cria o tópico remoto necessário e mensagens destinadas a assinantes remotos são encaminhadas como `REPASSE`. Criação e exclusão de tópicos também são comunicadas ao par.

Essa comunicação não é consenso nem replicação completa de estado. Cada broker administra suas próprias sessões, filas e cache; tópicos e interesses podem divergir durante falhas ou partições. O repasse é limitado ao par, não forma uma rede geral de brokers.

## Biblioteca cliente

O pacote `client` oferece `Conectar` e `ConectarComConfig`, as operações de tópico e publicação, `Eventos()` para consumir publicações e avisos, `Estatisticas`, `BrokerAtual`, `Taxa`, `Descartadas` e `Fechar`. A configuração recebe um ou dois endereços UDP.

Quando deixa de receber comunicação válida, o cliente tenta o outro broker configurado, usando espera crescente com variação aleatória entre rodadas sem sucesso. Depois da troca, reenvia as inscrições confirmadas antes de considerar a conexão ativa. O cliente não transfere um pedido administrativo pendente ao segundo broker: como os caches de deduplicação são locais, isso poderia executar a operação duas vezes.

Publicações recebidas são deduplicadas em uma janela limitada. A fila local de dados também tem capacidade limitada e descarta as mensagens mais antigas quando cheia; `Descartadas()` permite consultar esse contador.

## Controle de tráfego adaptativo

O controle existente limita a taxa de publicações e entregas com token buckets e comunica ocupação e contadores entre cliente e broker. O cliente inicia no teto local configurado, respeitando o teto informado pelo broker. Pressão, sinais de perda ou ausência de feedback recente reduzem a taxa adaptativa pela metade; duas avaliações boas consecutivas, com demanda, aumentam uma taxa reduzida em 10%, sem ultrapassar o teto. A alteração da taxa não renova a rajada disponível.

No broker há limites por identificador de cliente e limites globais de entrada e saída, além de orçamentos próprios para controle e repasse entre brokers. Os valores podem ser configurados por flags. Defaults relevantes: 100 publicações/s por cliente, 2.000 datagramas/s de entrada, 5.000 datagramas/s de saída, 200 datagramas/s de controle, 1.000 repasses/s e rajada 4. São limites do protótipo, não uma garantia de justiça ou um controle de congestionamento validado para a Internet.

## Organização do repositório

```text
client/                  Biblioteca pública, documentação e testes do cliente
cmd/broker/              Executável do broker UDP
cmd/carga/               Gerador de carga e exportação dos resultados
cmd/demo/                Demonstração interativa e modos de operação
cmd/reconexao/           Demonstração de publicação e failover do cliente
cmd/testclient/          Cliente simples para teste manual
internal/avaliacao/      Execução de carga, métricas e relatórios
internal/broker/         Estado e lógica do broker
internal/controle/       Token bucket e controle adaptativo
internal/fila/           Filas limitadas
internal/recente/        Cache em memória de dados recentes
protocol/                Mensagens JSON e validação do protocolo
```

## Executar

Execute os comandos a partir da raiz do repositório. Para iniciar um broker:

```sh
go run ./cmd/broker -endereco 127.0.0.1:8080
```

Para executar um par, use dois terminais:

```sh
go run ./cmd/broker -endereco 127.0.0.1:8080 -par 127.0.0.1:8081 -id b1
go run ./cmd/broker -endereco 127.0.0.1:8081 -par 127.0.0.1:8080 -id b2
```

Com um broker ativo, execute os comandos abaixo em terminais separados:

```sh
go run ./cmd/demo -modo assistir -topico jogo/reacoes
go run ./cmd/demo -modo publicar -topico jogo/reacoes -mensagem "Gol!" -intervalo 100ms
go run ./cmd/demo -modo interativo
```

O modo interativo aceita `criar`, `apagar`, `assinar`, `cancelar`, `publicar` e `sair`, com o tópico como segundo campo. Também há modos `criar`, `apagar`, `cancelar`, `publicar` e `estatisticas` para operações individuais; `assistir` assina o tópico e fica aguardando mensagens. Para escolher dois brokers, use `-brokers host1:porta,host2:porta`.

As demonstrações específicas do cliente são:

```sh
go run ./cmd/reconexao -brokers 127.0.0.1:8080,127.0.0.1:8081 -topico leilao/lance
go run ./cmd/testclient -brokers 127.0.0.1:8080,127.0.0.1:8081 -topico leilao/lance -mensagem "100"
```

O primeiro publica periodicamente e exibe mudanças de broker. O segundo assina o tópico, publica uma mensagem de teste e aguarda uma publicação recebida.

### Gerar uma avaliação

O gerador cria publicadores e assinantes, executa o cenário pelo período configurado e salva `resumo.json`, `intervalos.csv`, `vazao.svg`, `latencia.svg`, `taxa.svg` e `relatorio.html` no diretório indicado por `-saida`.

```sh
go run ./cmd/carga -topico teste/normal -publicadores 2 -assinantes 10 -taxa 30 -duracao 10s -saida resultados/normal
go run ./cmd/carga -topico teste/lento -publicadores 2 -assinantes 10 -lentos 1 -atraso-lento 100ms -buffer-cliente 4 -taxa 30 -duracao 15s -saida resultados/lento
go run ./cmd/carga -topico teste/agressivo -publicadores 1 -assinantes 10 -taxa 1000 -ignorar-controle -duracao 10s -saida resultados/agressivo
go run ./cmd/carga -brokers 127.0.0.1:8080,127.0.0.1:8081 -distribuir -topico teste/dois -duracao 10s -saida resultados/dois
```

`-ignorar-controle` é uma opção do gerador para testar a proteção do broker contra clientes que excedem o controle local. As medições são experimentais: entregas ausentes podem ter diversas causas e não medem, por si só, perda de rede, uso de CPU ou memória.

## Testes e validação

A coleta opcional de CPU, RAM e filas do broker usa `-metricas arquivo.csv`. As filas locais/do gerador também são registradas, e o resumo separa duplicatas filtradas pela biblioteca das detectadas na aplicação. Para repetir os cinco cenários e gerar comparações, siga o [guia da avaliação](experimentos/README.md).

```sh
go test ./... -count=1
go test ./client -count=5
go vet ./...
go build ./...
```

Os testes cobrem serialização e validação do protocolo, operações e publicação, filas e consumidores lentos, limites de taxa, avisos e confirmações, repasse entre dois brokers, failover e reconstrução de inscrições. Os testes do cliente também exercitam respostas UDP, retries com o mesmo ID, descarte de duplicatas, resultado administrativo desconhecido, filas locais e brokers independentes.

## Limitações conhecidas

- UDP não garante entrega, ordem ou ausência de duplicatas. Publicações não são armazenadas para reenvio após falha.
- O broker guarda o cache administrativo em memória, limitado a 2.048 entradas com validade nominal de 20 segundos. A deduplicação de publicações é limitada a 8.192 entradas por broker, também com expiração nominal de 20 segundos; os caches do cliente também são finitos. Entradas podem ser expulsas antes do prazo. Não há garantia de execução exatamente uma vez.
- Se um pedido administrativo foi enviado sem confirmação, o cliente retorna `ErrResultadoDesconhecido`. Ele não afirma sucesso ou falha e não reenvia automaticamente o pedido a outro broker.
- O failover restaura inscrições confirmadas do cliente, não replica o estado completo entre brokers. Tópicos e inscrições podem divergir em falhas ou partições.
- O armazenamento é em memória; reiniciar processos perde tópicos, filas, sessões e inscrições.
- Não há autenticação ou confidencialidade. O projeto pressupõe endereços conhecidos e uma rede acadêmica controlada.
- Os resultados de `cmd/carga` descrevem a execução observada e a configuração usada; não são certificação de capacidade de produção nem desempenho em outras redes ou equipamentos.

## Integrantes e responsabilidades

- **Maria Luiza:** biblioteca cliente Go, comunicação UDP/JSON do cliente, failover e reconstrução das inscrições locais, testes do cliente e demonstrações de terminal.
- **Felipe Macedo:** broker, gerenciamento de tópicos e inscrições, distribuição de publicações, filas e descartes, comunicação e repasse entre brokers.
- **José Correia:** limitadores e controle adaptativo de taxa, gerador de carga, coleta de métricas, gráficos e análise dos resultados.

As responsabilidades não substituem a integração: o cliente depende do protocolo e das respostas do broker; o comportamento de tráfego e a avaliação dependem dos componentes correspondentes.
