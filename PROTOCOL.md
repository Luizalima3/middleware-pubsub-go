# Protocolo UDP/JSON

Cada datagrama contém **um objeto JSON UTF-8**, com até 1.000 bytes. Tópico: até 80 bytes; ID de cliente e origem: até 64 bytes. Mensagem maior é recusada, sem fragmentação na aplicação. Campos desconhecidos, JSON inválido ou dados maiores que o limite são ignorados no recebimento.

Exemplo legível:

```json
{"tipo":"PUBLICAR","id":42,"cliente":"ana","topico":"jogo/reacoes","conteudo":"Gol!"}
```

## Campos

| Campo | Uso |
| --- | --- |
| tipo | Operação ou resposta. |
| id | ID inteiro de 64 bits do pedido/publicação. O cliente usa um contador iniciado aleatoriamente. |
| cliente | ID do participante, declarado pela biblioteca. Compartilha a cota das sessões no mesmo broker. |
| topico | Assunto; vazio só em mensagens que não dependem de tópico. |
| conteudo | Texto UTF-8. Na API Go é []byte, mas não aceita conteúdo binário arbitrário. |
| origem | Broker que originou uma entrega, aviso ou repasse. |
| aviso | ID de uma notificação que exige confirmação. |
| taxa | Teto de envio informado pelo broker. |
| fila | Percentual da fila do cliente ou sinal de pressão do broker. |
| recebidas, enviadas, perdidas | Contadores acumulados usados no feedback, não confirmações individuais. |

## Operações do cliente

- CRIAR_TOPICO: idempotente; também OK se o tópico existe.
- APAGAR_TOPICO: libera fila e inscrições locais, avisa inscritos e comunica o outro broker.
- ASSINAR: cria o tópico se necessário, registra o endpoint e comunica interesse.
- CANCELAR: remove inscrição e mensagens ainda na fila desse cliente para o tópico.
- PUBLICAR: sem confirmação positiva e sem reenvio automático.
- ATIVIDADE: informa contadores e ocupação da fila; recebe ESTADO.
- ESTATISTICAS: solicita contadores agregados do broker.
- RECEBIDO: confirma o campo aviso de uma notificação.

Operações administrativas recebem OK ou ERRO com o mesmo id. A API aguarda a resposta antes de retornar; só um comando administrativo fica pendente por cliente. Em caso de timeout, repete o pedido com o mesmo ID até o limite de tentativas. O cache de respostas é limitado e expira em 20 segundos. Reutilizar um ID recente para outra operação/tópico retorna ID_REUTILIZADO.

Uma entrega ao consumidor usa MENSAGEM e preserva cliente, id, tópico e conteúdo. O cliente reconhece duplicatas recentes por (cliente, id), com memória limitada. Não existe ordem total nem histórico de publicações.

## Avisos

SEM_INSCRITOS indica ausência de inscritos locais e de interesse remoto conhecido. DESCARTADAS traz no conteúdo uma contagem agregada de entregas removidas da fila do broker. RATE_LIMIT é um ERRO de admissão; outros erros incluem TOPICO_INVALIDO, TOPICO_INEXISTENTE, TOPICO_SOBRECARREGADO, MENSAGEM_GRANDE e LIMITE_TOPICOS. TOPICO_APAGADO avisa a exclusão.

Avisos importantes recebem aviso, são guardados numa tabela limitada e repetidos até RECEBIDO ou esgotamento das tentativas. O recebimento pela biblioteca não prova que o usuário leu o aviso. A biblioteca reserva uma fila administrativa; quando ela está cheia, deixa de confirmar a nova notificação para permitir a repetição. Se as tentativas acabarem ou o orçamento/tabela estiver cheio, o aviso pode não chegar.

Dados não usam esse mecanismo. O silêncio após PUBLICAR não comprova aceite ou entrega.

## Comunicação com o par

Somente datagramas do endpoint do par configurado são tratados como inter-broker. INTERESSE anuncia a quantidade de inscritos locais no tópico e é renovado periodicamente. Interesses expiram por inatividade.

CRIAR_REMOTO e APAGAR_REMOTO usam confirmação por aviso e não são repassados de novo. REPASSE leva uma publicação aos inscritos locais, sem novo encaminhamento. IDs recentes evitam duplicatas; cada broker comunica apenas interesse local, nunca o interesse do outro como se fosse seu.

O par tem fila limitada e orçamento de tráfego próprios. Atualizações podem se perder temporariamente; não há consenso para resolver administrações concorrentes durante uma partição.

## Detecção e recuperação

ATIVIDADE/ESTADO permitem perceber falta de resposta. O cliente considera também a última comunicação válida do broker, tenta o próximo da lista e reenvia ASSINAR. Publicações durante indisponibilidade falham com ErrDesconectado. Não são acumuladas para retransmissão após failover.

Parâmetros padrão do cliente: atividade 1 s; indisponibilidade percebida após 3 s sem mensagens; três tentativas de comando, espaçadas por 250 ms. No broker, avisos são tentados três vezes, com 300 ms, e sessões/interesses expiram após 5 s. São configurações iniciais de teste, não metas garantidas.

## Limites de aplicação

Mensagem individual: 1.000 bytes; fila de tópico: 128 mensagens/128.000 bytes; fila do consumidor no broker: 64 mensagens/64.000 bytes; leitura do cliente: 32 mensagens, até 1.000 bytes cada; fila administrativa do cliente: 64 mensagens.

Por padrão: 256 sessões, 128 tópicos, 512 avisos pendentes, cache de controle de 2.048 entradas e cache de publicações de 8.192 entradas, com expiração. Os caches são recentes e podem expulsar entradas por capacidade antes do prazo. O limite por ID é local a um broker; usar outro ID ou outro broker não é uma identidade autenticada/global.

Referências técnicas: RFC 768 (UDP), RFC 8085 (uso responsável de UDP), RFC 8259 (JSON). O algoritmo de ajuste é uma regra experimental para rede local, não uma implementação de um padrão de congestionamento para Internet.

Configure o período de atividade do cliente abaixo de metade do prazo de inatividade do broker. O período de manutenção/anúncios do broker pode ser mais curto e não é usado como prazo de ausência de feedback.
