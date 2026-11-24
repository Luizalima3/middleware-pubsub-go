# Avaliação: CPU, memória, filas e duplicatas

Esta parte mede o processo do broker e repete cinco cenários com parâmetros registrados. O protocolo UDP e as regras de tráfego continuam os mesmos.

O [lote final publicado](resultados/README.md) já contém os gráficos, CSVs, análise e dados das 15 execuções. Para apenas consultar os resultados, não é necessário executar os comandos abaixo.

## Rodar tudo

Precisa de Go, Python 3.10 ou superior e Matplotlib para os gráficos. Os executáveis Go continuam sem dependências externas. Na raiz do projeto:

```sh
python -m pip install -r scripts/requirements.txt
python scripts/experimentos.py --saida resultados/comparacao-01
```

O script testa e compila os executáveis, inicia brokers novos, repete cada cenário três vezes e gera a comparação. Use uma pasta nova a cada lote: resultados anteriores não são sobrescritos. Os cenários usam as mesmas cargas da verificação anterior, agora com coleta em intervalos de 200 ms. A configuração está em configuracao.json; a ordem é alternada com uma semente fixa.

Para executar apenas a coleta sem instalar a biblioteca de gráficos:

```sh
python scripts/experimentos.py --saida resultados/comparacao-02 --sem-graficos
```

Depois, os gráficos podem ser refeitos sem repetir a carga:

```sh
python scripts/comparar.py --entrada resultados/comparacao-02
```

`--go caminho/do/go` escolhe o compilador. `--sem-testes` só deve ser usado se a mesma revisão já foi verificada. Os processos são locais, sem outras cargas intencionais concorrentes; outros programas da máquina podem interferir nos tempos. O script também funciona em um ZIP extraído: se não houver metadados Git, a revisão fica vazia e os hashes dos arquivos-fonte identificam o código.

## O que é medido

| Medida | Como é obtida | Onde fica |
| --- | --- | --- |
| CPU do broker | Diferença de tempo de usuário + kernel / tempo decorrido | broker-N.csv |
| RAM do broker | Memória residente atual do processo | broker-N.csv |
| Filas do broker | Itens, bytes, limites e picos por tópico/sessão; controle e par | broker-N.csv |
| Filas locais | Dados/avisos da biblioteca e fila de envio do gerador | filas-clientes.csv |
| Duplicatas filtradas | Contador Client.Duplicadas(), antes de Eventos() | resumo.json |
| Duplicatas no gerador | Repetição do identificador da publicação após Eventos() | resumo.json |

CPU de 100% significa um núcleo lógico ocupado. Pode passar de 100%. O CSV também inclui o valor dividido pelo número de CPUs lógicas: `cpu_pct_maquina`. A primeira amostra não tem percentual porque ainda não há um intervalo anterior. RAM é o working set no Windows e a parte residente de /proc/self/statm no Linux; não é apenas heap Go. A coleta nativa é implementada para Windows e Linux; outros sistemas podem executar o broker sem `-metricas`.

Fontes das medidas Windows: [GetProcessTimes](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getprocesstimes), [GetProcessMemoryInfo](https://learn.microsoft.com/en-us/windows/win32/api/psapi/nf-psapi-getprocessmemoryinfo) e [WorkingSetSize](https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters_ex).

As filas de dados mantêm picos desde sua criação, atualizados em cada inserção. Assim, uma rajada pode aparecer no pico mesmo que termine entre amostras. As filas locais e a fila de controle têm somente amostragem de ocupação. Campos de bytes/picos não medidos na fila de controle ficam vazios. Itens já retirados para envio não estão na fila; `em_envio` reaproveita o sinal de espera no limitador local do consumidor e não mede todos os itens em trânsito.

Para coletar manualmente:

```sh
go run ./cmd/broker -endereco 127.0.0.1:8080 -metricas resultados/manual/broker.csv -intervalo-metricas 200ms
go run ./cmd/carga -duracao 10s -saida resultados/manual -intervalo-metricas 200ms
```

## Arquivos da comparação

- relatorio-comparativo.html: gráfico geral, explicação e séries da primeira repetição.
- ANALISE.md: análise em texto, com mediana, mínimo e máximo por cenário.
- comparativo.csv: uma linha por execução; comparativo.json inclui cobertura de CPU por broker.
- comparativo.png e comparativo.svg: gráficos para apresentação.
- series-CENARIO-NN.png: CPU, RAM e filas de cada repetição.
- manifesto.json: ambiente, ordem, revisão base, patch e hashes dos binários/scripts/configuração.
- fonte.patch: alterações em relação à revisão base; pode ser vazio se a revisão estiver salva em commit.
- Pastas dos cenários: resumo, CSVs, logs, comandos executados e contadores finais.

CPU é comparada pelo tempo acumulado em intervalos inteiramente dentro da janela de carga+drenagem, dividido pela duração observada. A cobertura por broker é explícita; bordas de amostragem e a última fração antes de um término forçado não são extrapoladas. RAM agregada é a soma de picos amostrados por processo, que podem não ser simultâneos nem representar páginas físicas exclusivas.

## Duplicatas: a correção feita

Antes, o gerador só contava repetições que chegassem por Eventos(). A biblioteca já descartava algumas delas silenciosamente. Agora o resumo separa `duplicatas_descartadas_pelo_cliente` e `duplicatas_detectadas_pelo_gerador`, e `entregas_duplicadas` é a soma. Cada recebimento removido na primeira etapa não chega à segunda.

O teste TestDuplicatasFiltradasPeloClienteChegamAoResumo injeta três cópias de cada mensagem e verifica três entregas únicas e seis duplicatas para três publicações. Isso verifica a contabilização; não simula uma taxa de duplicação espontânea. Os caches são limitados, e esses contadores não medem duplicatas descartadas antes do cliente.

## Como apresentar sem exagerar a conclusão

Cada cenário repete sua própria configuração. Normal, lento, excesso, dois brokers e falha têm cargas diferentes entre si, preservadas na configuração; a comparação é descritiva. As barras mostram mediana e as hastes mínimo–máximo, sem teste de significância. Ausências, rejeições e descartes não são causas independentes e não devem ser somados.

No cenário lento, examine os consumidores normais separadamente do atraso artificial. No excesso, o cliente ignora o limitador local para testar o broker. Na falha, o primeiro broker é terminado dois segundos após lançar o gerador; a retomada é verificada, mas não se mede automaticamente o tempo exato de indisponibilidade. Todas as repetições usam instrumentação; seu custo adicional não foi isolado. Os testes locais não comprovam capacidade de produção.
