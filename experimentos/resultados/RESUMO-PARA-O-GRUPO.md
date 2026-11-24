# O que foi concluído na avaliação

Agora medimos quanto processador e memória cada broker usa. Também registramos quantas mensagens ficam nas filas, os limites dessas filas e os descartes. Os dados ficam em CSV, e os gráficos são gerados a partir desses arquivos.

Repetimos cada um dos cinco cenários três vezes. Dentro de cada cenário, usamos a mesma configuração. Foram 15 execuções no lote final.

## O que podemos explicar

- **Normal:** as três execuções tiveram 270 entregas e nenhuma entrega ausente. O p99 dos consumidores normais ficou entre 27,49 e 32,85 ms.
- **Consumidor lento:** uma fila de consumidor chegou a 100% da capacidade e houve descartes. A memória residente teve pico mediano de aproximadamente 13,14 MiB. Os consumidores normais também tiveram atraso maior nesse cenário: p99 mediano de 191,41 ms. Não devemos dizer que o atraso foi completamente isolado; a carga e o buffer também mudam em relação ao cenário normal.
- **Excesso de envio:** o cliente enviou acima do permitido para testar a proteção do broker. Houve rejeições. As entregas ausentes não são todas perda de rede: incluem publicações rejeitadas.
- **Dois brokers:** houve repasse entre eles, com 180 entregas e nenhuma ausência nas três repetições. A memória mostrada no comparativo soma os picos dos dois processos; não é o consumo de um único broker.
- **Queda de broker:** encerramos o primeiro processo durante o experimento. Os clientes retomaram a comunicação pelo segundo, mas houve interrupção e mensagens ausentes. Não garantimos recuperação sem perdas.

## E as duplicatas?

Antes, algumas mensagens repetidas eram eliminadas pela biblioteca cliente e não apareciam na contagem do gerador. Agora contamos as duas etapas separadamente e somamos os resultados.

Nos 15 experimentos finais, não foram detectadas duplicatas. Para conferir se o contador funciona, um teste envia três cópias de cada uma de três publicações: o resultado foi três entregas únicas e seis duplicatas filtradas.

## O cuidado com as conclusões

São testes locais e curtos. CPU de 100% representa um núcleo lógico, e não todo o computador. RAM é memória residente do processo. Os gráficos mostram mediana e variação das três execuções; não provam capacidade de produção nem explicam sozinhos a causa de cada atraso.

A implementação passou em 47 testes Go, na análise do código e na compilação. Os resultados e os arquivos brutos estão neste pacote. O guia de execução está em experimentos/README.md no código-fonte.
