# Verificação desta entrega

Base: main 27449d3, com a biblioteca cliente atualizada pela equipe. A coleta e a análise foram implementadas na branch feat/metricas-recursos-experimentos.

- 47 testes Go passaram, incluindo os quatro novos testes de CPU/CSV, pico de fila e duplicatas.
- go vet ./... passou.
- go build ./... passou no Windows/amd64.
- Compilação para Linux/amd64 passou; a coleta não foi executada em Linux nesta verificação.
- Scripts Python conferidos; a execução a partir de fonte sem metadados Git é aceita.
- A coleta de RAM usa o processo real. O teste do CSV verifica a primeira amostra sem percentual de CPU e a proteção contra sobrescrever resultados anteriores.
- O teste de duplicação controlada confirmou 3 entregas únicas e 6 duplicatas filtradas para 3 publicações enviadas em triplicata.

## Experimentos

O lote final contém três repetições de normal, lento, excesso, dois brokers e falha, totalizando 15 execuções. Os parâmetros estão em configuracao.json. Foram usados processos novos, a mesma configuração por cenário e ordem alternada com semente fixa.

Os testes foram executados separadamente antes do lote. Por isso o comando de coleta usou --sem-testes, e o manifesto registra testes_executados=false para o orquestrador. Isso não significa que os testes Go tenham sido dispensados na entrega.

Uma rodada piloto foi preservada separadamente e excluída dos resultados finais: houve compilação de verificação concorrente a parte dela. O lote final foi repetido depois que essa compilação terminou. A análise final usa exclusivamente a pasta experimentos-recursos-2026-11-23-final.

Todos os cenários terminaram com comunicação útil, limites de fila respeitados, CPU/RAM disponíveis e configurações iguais entre as três repetições. Não houve linhas incompletas nos CSVs do lote final. Os casos de excesso, consumidor lento, repasse e retomada no sobrevivente foram efetivamente exercitados.

## Limites

São experimentos curtos e locais. O custo da instrumentação não foi isolado. CPU tem cobertura temporal registrada por broker, sem extrapolar bordas de amostragem. A soma dos picos de RAM não é memória física exclusiva nem necessariamente simultânea. Não foi executado o detector -race, pois o ambiente não tem CGO e compilador C configurados.

O cenário lento também apresentou aumento de p99 dos consumidores normais; a análise não afirma isolamento perfeito. As cargas dos cenários são diferentes, então os resultados não demonstram causalidade de um único componente. Novos ajustes de desempenho exigem uma comparação controlada adicional.
