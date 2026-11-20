# Validação local da adaptação UDP

Verificação executada em 20/11/2026, em Windows/amd64 com Go 1.27.3. A versão mínima declarada pelo módulo é Go 1.23; não foi executada uma matriz de versões.

## Verificações concluídas

- go test ./...: 26 testes passaram.
- go vet ./...: passou.
- go build de todos os comandos: passou.
- Biblioteca pública compilada por outro módulo Go, com replace local: passou.
- Cliente interativo em processo separado: criar, assinar, publicar, receber, cancelar e apagar passaram.
- git diff --check: sem problemas após normalização dos arquivos de texto.
- Testes de integração usam sockets e brokers UDP reais em loopback, incluindo perda proposital de confirmação e timeout com resultado desconhecido.
- Um teste de regressão cobre manutenção do broker mais rápida que o feedback do cliente. O período de manutenção não deve causar redução indevida da taxa.

O detector go test -race ./... não foi executado: o Go informou que -race exige CGO, desativado neste ambiente. Não há compilador C disponível no PATH. Os testes normais de concorrência passaram, mas não substituem o detector de corrida.

## Execução dos programas compilados

Cada cenário usou processos novos para o broker, evitando herdar sessões e cotas dos anteriores. Mensagens de teste: payload de 128 bytes, com metadados ASCII dentro do JSON. A coluna de entregas conta os pares publicação/assinante; pode ser maior que a quantidade de publicações.

| Cenário | Escritas UDP sem erro | Entregas únicas | Entregas ausentes | p99 geral (ms) |
| --- | --- | --- | --- | --- |
| normal | 90 | 270 | 0 | 34.178 |
| lento | 576 | 635 | 517 | 1093.404 |
| excesso | 900 | 194 | 1606 | 284.426 |
| dois-brokers | 60 | 180 | 0 | 35.559 |
| falha | 104 | 106 | 102 | 2773.247 |

Esses números são registros de uma verificação funcional curta, não metas de latência nem resultados de capacidade de produção. As ausências incluem rejeições, descartes, atrasos e falhas, sem identificação automática de causa.

- Normal: 90 publicações para três inscritos, 270 entregas e nenhuma ausência nesta execução.
- Lento: houve pressão real nas filas e descartes registrados. O p99 geral mistura consumidores normais e lentos; consulte os percentis separados no resumo.
- Excesso: o cliente ignorou o controle local; o broker recusou publicações acima da cota.
- Dois brokers: houve encaminhamento remoto em um salto, com contadores de repasse e nenhuma ausência nesta execução.
- Falha: o primeiro broker foi terminado durante a carga. O segundo aceitou novas publicações e a comunicação foi retomada, com interrupção e ausências observadas. O teste não determina automaticamente a duração exata dessa interrupção.

O diretório irmão validacao-udp contém, para cada cenário, configuração, resumo, contadores do broker, CSV, SVGs e HTML. resultado.json registra o sistema, número de CPUs lógicas, data e resumo das execuções. Não foram coletadas medidas de CPU e memória do broker para essa verificação.

## O que ainda deve ser avaliado pela equipe

- Repetir experimentos no hardware e na rede usados na apresentação.
- Definir metas de latência/vazão e calibrar intervalos, tetos e filas.
- Medir CPU, memória e tempo exato de indisponibilidade.
- Executar -race em ambiente com CGO e compilador C.
- Avaliar perda/reordenação em rede e particionamento entre brokers, além dos casos automatizados locais.
- Confirmar com o professor a interpretação de entrega como tentativa de envio e descarte sob sobrecarga.

A branch do núcleo WebSocket encontrada no repositório foi preservada. Esta adaptação partiu da main fc1eaa6 em uma nova branch e mantém o histórico anterior.
