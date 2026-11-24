# Resultados dos experimentos

Foram concluídas 15 execuções: 3 por cenário. Mesma configuração dentro de cada cenário; processos novos em cada repetição. Ordem alternada com semente registrada.

## Como interpretar

CPU usa o tempo de usuário + kernel do próprio broker. 100% corresponde a um núcleo lógico; pode ultrapassar 100%. A soma do tempo de CPU dos intervalos inteiros cobertos é dividida pela janela do experimento. Os intervalos de borda não são extrapolados: a cobertura exata de cada broker está em comparativo.json. Na falha, a coleta do broker terminado acaba antes da janela.

RAM é o conjunto residente (working set no Windows), não o heap Go. A comparação soma o maior valor amostrado de cada broker; esses picos podem ocorrer em instantes diferentes e páginas compartilhadas podem ser contadas em ambos. Não é a RAM física exclusiva da máquina.

As filas do broker registram ocupação atual e picos desde sua criação, por quantidade e bytes. Os picos das filas de dados são atualizados a cada inserção, inclusive entre amostras; controle tem apenas ocupação amostrada em itens. Filas locais e do gerador são amostradas, podendo perder picos rápidos. Um item já retirado para envio não está na ocupação; em_envio indica a espera no limitador local, sem medir todos os itens em trânsito.

As cargas e durações diferem entre cenários. Esta é uma comparação descritiva, não um teste que isola o efeito do algoritmo. Os p99 são calculados por execução: a mediana desses percentis não é o p99 de todas as mensagens reunidas. As hastes mostram variação entre repetições, não significância estatística.

## Comparação (mediana [mínimo–máximo])

| Cenário | CPU registrada/janela (%) | RAM, soma picos (MiB) | Pico fila consumidor (%) | p99 normais (ms) | Ausentes (%) |
| --- | --- | --- | --- | --- | --- |
| normal | 0.78 [0.39–0.78] | 9.99 [9.98–10.17] | 3.12 [3.12–3.12] | 27.49 [27.49–32.85] | 0.00 [0.00–0.00] |
| lento | 7.03 [5.73–8.85] | 13.14 [13.09–13.19] | 100.00 [100.00–100.00] | 191.41 [183.98–199.14] | 44.90 [44.90–44.91] |
| excesso | 9.77 [9.76–10.93] | 13.22 [13.11–13.55] | 12.50 [12.50–12.50] | 290.11 [290.11–295.92] | 89.22 [89.22–89.22] |
| dois-brokers | 1.95 [0.39–2.34] | 19.25 [19.23–19.38] | 1.56 [1.56–1.56] | 34.86 [32.85–38.49] | 0.00 [0.00–0.00] |
| falha | 0.35 [0.17–0.52] | 18.80 [18.77–18.86] | 1.56 [1.56–1.56] | 3517.15 [2773.25–3883.22] | 44.44 [39.56–49.04] |

## O que foi observado

- **normal:** entregas únicas 270.00 [270.00–270.00]; ausentes 0.00 [0.00–0.00]%; p99 dos consumidores normais 27.49 [27.49–32.85] ms. Maior pico de fila de consumidor: 3.1%.
- **lento:** entregas únicas 637.00 [637.00–638.00]; ausentes 44.90 [44.90–44.91]%; p99 dos consumidores normais 191.41 [183.98–199.14] ms. Maior pico de fila de consumidor: 100.0%.
- **excesso:** entregas únicas 194.00 [194.00–194.00]; ausentes 89.22 [89.22–89.22]%; p99 dos consumidores normais 290.11 [290.11–295.92] ms. Maior pico de fila de consumidor: 12.5%.
- **dois-brokers:** entregas únicas 180.00 [180.00–180.00]; ausentes 0.00 [0.00–0.00]%; p99 dos consumidores normais 34.86 [32.85–38.49] ms. Maior pico de fila de consumidor: 1.6%.
- **falha:** entregas únicas 106.00 [90.00–110.00]; ausentes 44.44 [39.56–49.04]%; p99 dos consumidores normais 3517.15 [2773.25–3883.22] ms. Maior pico de fila de consumidor: 1.6%.

Os consumidores normais tiveram p99 mediano de 27.49 ms no cenário normal e 191.41 ms no cenário lento. Assim, não se deve afirmar que o atraso dos normais foi eliminado. A taxa oferecida e o buffer local também diferem entre esses casos: os dados não isolam o consumidor lento como única causa. A avaliação mostra o comportamento observado; calibrar os limites e realizar um teste com apenas um fator alterado é o próximo passo se a equipe quiser demonstrar isolamento de latência.

O cenário lento inclui atraso artificial e filas menores no cliente. Seu p99 geral mistura consumidores lentos e normais; por isso a comparação usa também o p99 separado dos normais. Ausências, avisos de rejeição e descartes podem se sobrepor e não devem ser somados como causas independentes.

O cenário de excesso ignora deliberadamente o controle local para verificar a proteção do broker. A redução da taxa informada nesse caso não significa que o cliente agressivo a respeitou. Os dois brokers geram custo agregado de dois processos; não compare essa soma como se fosse o consumo de apenas um processo.

Na falha, o primeiro broker é terminado dois segundos após lançar o processo do gerador, não após o início exato das publicações. O instante real do evento está em execucao.json e nos gráficos. O teste verifica retomada no sobrevivente; não mede automaticamente o tempo exato de indisponibilidade.

## Duplicatas

As 15 execuções registraram 0 duplicatas filtradas pela biblioteca e 0 detectadas pelo gerador. O total é a soma das etapas: uma mensagem removida antes de Eventos() não é contada novamente pelo gerador.

Há um teste de injeção controlada que envia três cópias de cada entrega e verifica três entregas únicas e seis duplicatas filtradas para três publicações. Esse teste demonstra a contabilização; não é evidência de duplicação espontânea na rede. Zero duplicatas observadas não prova ausência fora da janela/cache nem mede as eliminadas pelo broker.

## Limites e reprodução

Resultados locais curtos, com instrumentação habilitada em todas as execuções. Não foi quantificado o custo adicional da coleta. Não há promessa de capacidade de produção nem conclusão sobre outras redes ou equipamentos. Aumente duração e repetições para a apresentação, usando uma nova pasta para cada lote.

configuracao.json guarda parâmetros; manifesto.json registra ambiente, ordem, versão Go, revisão base, patch e hashes dos binários. Cada repetição contém CSVs, logs, resumo, contadores finais e comandos completos. comparativo.csv tem uma linha por execução; comparativo.json inclui a cobertura de CPU por broker. As séries usam tempo relativo ao início da carga e incluem a drenagem.
