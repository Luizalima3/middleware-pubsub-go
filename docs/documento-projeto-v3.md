# Middleware Pub/Sub

Documento de projeto simplificado - Cenário C
Etapa 1: proposta para discussão com o professor

IFPB - Campus João Pessoa | Programação Distribuída | Prof. Ruan Delgado Gomes
Equipe: Felipe Macedo, José Correia e Maria Luiza
Revisão da proposta: 5 de novembro de 2026

### 1. O que queremos construir

Um programa para distribuir mensagens de um evento ao vivo. Um participante publica uma reação em um assunto, chamado tópico, e os espectadores inscritos nesse tópico recebem a mensagem. O exemplo de demonstração será a troca de reações durante um jogo.

A prioridade é entregar mensagens recentes com pouco atraso e reduzir o efeito de clientes que publicam demais ou recebem devagar. Perdas durante sobrecarga são aceitas na proposta e serão medidas nos testes.

### 2. Quais partes o sistema terá

| Parte | Responsabilidade |
| --- | --- |
| Aplicação no terminal | Permitir publicar, receber mensagens e administrar tópicos. |
| Biblioteca cliente em Go | Preparar mensagens, controlar o ritmo de envio e tentar outro broker quando necessário. |
| Broker em Go | Guardar tópicos e inscrições, limitar filas e distribuir mensagens. |
| Dois brokers | Trocar informações e repassar publicações entre as duas instâncias. |

Diagrama: publicador -> biblioteca cliente -> broker -> consumidores inscritos. Transporte: UDP.

A demonstração funcionará no terminal. A comunicação será por UDP. A proposta é para testes em uma rede local controlada, com mensagens pequenas e parâmetros configuráveis.

> Este documento descreve o plano. A mudança de transporte e o controle adaptativo ainda precisam ser implementados e avaliados. Não há resultados de desempenho apresentados como comprovados.

## 3. Como as mensagens vão circular

O cliente informa a operação, seu identificador, o tópico e, quando necessário, o conteúdo. Cada mensagem será enviada em um datagrama UDP. A proposta inicial usa JSON, um formato de texto que facilita ler as mensagens e descobrir erros.

{"tipo":"PUBLICAR", "id":42, "cliente":"ana",
 "topico":"jogo/reacoes", "conteudo":"Gol!"}

O limite inicial proposto é de 1.000 bytes por mensagem completa, incluindo os campos de controle. Mensagens maiores serão recusadas; a aplicação não dividirá mensagens em partes. Esse valor deverá ser verificado nos testes da rede utilizada.

| Operação | O que faz |
| --- | --- |
| CRIAR_TOPICO | Cria um tópico. Se já existir, informa sucesso. |
| APAGAR_TOPICO | Remove o tópico e suas inscrições, libera pendências e avisa os inscritos. |
| ASSINAR | Inscreve o cliente. Se o tópico não existir, cria antes de inscrever. |
| CANCELAR | Remove a inscrição. Repetir o cancelamento mantém o mesmo resultado. |
| PUBLICAR | Encaminha o conteúdo aos inscritos locais e, se houver interesse, ao outro broker. |

### Confirmações e perdas

Criar, apagar, assinar e cancelar recebem confirmação. Se ela não chegar, o cliente repete o pedido com o mesmo ID por um número limitado de tentativas. O broker guarda temporariamente o resultado desses pedidos para responder a repetições sem executar a operação outra vez.

As publicações não terão confirmação positiva nem reenvio automático. Cada uma leva um ID para identificar duplicatas em uma memória recente e limitada. Não prometemos entrega de todas as publicações nem ordem global entre clientes.

Se não houver inscritos locais nem interesse remoto conhecido, o broker descarta a publicação e envia SEM_INSCRITOS ao publicador. Esse aviso, como os demais avisos administrativos importantes, será confirmado pelo cliente e repetido por um número limitado de tentativas se necessário.

> Enviar um datagrama sem erro ou ficar sem resposta não comprova aceitação nem entrega. Se os pedidos e confirmações se perderem em todas as tentativas, a aplicação informa falha ou resultado desconhecido. A ciência de descarte é limitada por essa comunicação.

Para começar, haverá apenas um comando administrativo pendente por cliente. Quantidade de tentativas, prazos e tamanho dos registros temporários serão configuráveis e terão limites.

## 4. Controle de tráfego e filas

O envio terá uma taxa ajustável com teto máximo. Se não houver dificuldade, a taxa não será reduzida. Se aparecer acúmulo persistente, ela será reduzida; depois, poderá voltar a subir até o teto.

Diagrama: receptor informa situação -> emissor ajusta taxa -> envio dentro dos tetos -> nova avaliação.

| Situação observada | Regra inicial proposta |
| --- | --- |
| Fila cresce ou perdas se repetem | Reduzir a taxa de envio pela metade. |
| Duas avaliações seguidas com folga | Se a taxa foi reduzida e houver demanda, aumentar 10%, até o teto. |
| Situação estável | Manter a taxa atual. |
| Sem informações recentes do receptor | Reduzir o envio e verificar se o receptor continua ativo. |

Como ponto de partida, a avaliação será feita a cada segundo. O broker informa ao publicador sua fila e a taxa permitida. Cada consumidor informa ao broker se sua fila de leitura está acumulando e quais mensagens recebeu. A regra é aplicada tanto no envio de publicações quanto na entrega a um consumidor lento.

### Tetos e proteção do broker

Cada cliente terá um limite máximo e o broker terá limites globais de entrada e saída. A soma das taxas autorizadas não poderá exceder o orçamento configurado, repartido de forma equivalente entre clientes ativos, limitado pela demanda e pelo teto de cada um. O tráfego de controle e o repasse entre brokers também terão limites.

O cliente espaça seus envios em vez de disparar tudo de uma vez. O broker confere os limites por identificador de cliente e rejeita o excesso com RATE_LIMIT. Um token bucket com taxa ajustável pode aplicar esse limite, com uma capacidade pequena e explícita para rajadas.

### Filas pequenas e com limite

Haverá uma fila por tópico para absorver rajadas e uma fila de saída por consumidor. Cada envio será independente. Filas serão limitadas por quantidade de mensagens e por bytes. Na fila de um consumidor cheia, sai a mensagem mais antiga; na entrada do tópico cheia, a nova publicação é recusada com aviso ao publicador.

> As porcentagens, os intervalos, as taxas e os tamanhos de fila são regras iniciais para testar, não garantias de desempenho. O limite fixo é proteção; o ajuste por informações do receptor é o componente dinâmico. O identificador do cliente será considerado confiável apenas no ambiente acadêmico.

## 5. Dois brokers e recuperação

Usaremos somente dois brokers com endereços conhecidos. A biblioteca escolhe um deles; o usuário continua usando publicar e assinar sem controlar o servidor diretamente.

Diagrama: Ana -> Broker 1 -> Broker 2 -> Bia. Repasse em um salto; destino entrega localmente e não devolve o repasse.

### Repasse direto, sem uma rede de servidores

Cada broker anuncia os tópicos em que possui inscritos. Uma publicação aceita é entregue aos inscritos locais e repassada ao outro broker quando houver interesse conhecido. Ao receber um repasse, o destino entrega apenas aos seus inscritos: não repassa de volta.

Criação e exclusão de tópicos também são comunicadas. Pedidos administrativos entre brokers usam identificação, confirmação e repetição limitada. O resumo de interesses é reenviado periodicamente e após uma recuperação. Para simplificar a demonstração, as alterações de tópicos serão feitas uma de cada vez, sem administração concorrente nas duas instâncias.

### Como perceber uma falha usando UDP

UDP não cria uma conexão que avise sozinha que o outro programa caiu. Por isso, cliente e broker trocam mensagens periódicas de atividade. Se as respostas deixarem de chegar pelo prazo configurado, o cliente tenta o outro endereço. A falta de resposta indica indisponibilidade percebida; não identifica a causa exata.

As tentativas seguintes têm espera crescente e uma pequena variação aleatória para evitar que todos tentem ao mesmo tempo. Ao conseguir resposta, a biblioteca reenvia as inscrições. Como ASSINAR pode criar o tópico, a nova instância consegue reconstruir essa parte do estado.

### Limitações que a equipe assume

Tópicos e inscrições ficam em memória. Um reinício perde o estado local; mensagens antigas não são recuperadas. Durante falhas entre brokers, o conhecimento dos interesses pode ficar desatualizado e uma exclusão pode divergir temporariamente. A comunicação é de melhor esforço, sem promessa de recuperação instantânea ou ausência de perdas.

> UDP também pode perder, duplicar ou mudar a ordem de mensagens. O controle proposto limita a aplicação em uma rede local de testes; não constitui uma solução completa de congestionamento para a Internet. Um uso fora desse ambiente exigiria mecanismos adicionais e nova avaliação.

## 6. Requisitos e escolhas

A redução mantém os sete requisitos mínimos na proposta. A tabela indica onde cada um será tratado; o atendimento deverá ser verificado na implementação e nos testes.

| Requisito do enunciado | Como a proposta trata |
| --- | --- |
| 1. Mensagens com tópico | Toda publicação identifica o tópico. |
| 2. Publicar, assinar e cancelar | A biblioteca oferece essas três operações. |
| 3. Criar e apagar tópicos durante a execução | Comandos de criação e exclusão, comunicados aos dois brokers. |
| 4. Sem inscritos: descartar e avisar | SEM_INSCRITOS, com confirmação do aviso e repetição limitada. Falhas totais de comunicação continuam possíveis. |
| 5. Encaminhar aos inscritos e apagar | Tentativa de envio a todos os inscritos conhecidos; remoção após envio local ou descarte. Escrita UDP não confirma consumo. |
| 6. Absorver rajadas por tópico | Fila de entrada limitada por tópico, além das filas por consumidor. |
| 7. Mais de um broker, transparente ao cliente | Lista de dois endereços, repasse direto entre instâncias e recuperação pela biblioteca. |

### O que ganhamos e o que perdemos

| Escolha | Ganho | Custo |
| --- | --- | --- |
| UDP | Não exige conexão nem retransmissão automática das publicações. | Perdas, duplicatas e ordem precisam ser consideradas pela aplicação. |
| JSON legível | Facilita explicar e investigar mensagens. | Mais bytes e processamento que o formato binário anterior. |
| Filas limitadas e descarte | Controla o acúmulo e prioriza mensagens recentes. | Consumidores podem perder mensagens. |
| Taxa dinâmica com teto | Adapta o envio e limita a carga oferecida. | Exige informações do receptor e ajuste nos testes. |
| Dois brokers em memória | Permite comunicação entre instâncias com escopo pequeno. | Falhas podem causar interrupções e diferenças temporárias de estado. |

> Ponto a confirmar com o professor: interpretar o encaminhamento a todos como tentativa de envio, aceitando perdas do UDP e descarte por consumidor sob sobrecarga. Se for exigida entrega garantida, esta proposta precisará de um mecanismo adicional; esse requisito não deve ser declarado atendido sem essa validação.

## 7. Como vamos testar

A demonstração será no terminal: um cliente publica reações e outros acompanham o mesmo tópico. Um gerador cria usuários simulados para repetir os experimentos. Primeiro verificaremos as operações com um broker; depois, a comunicação e a recuperação usando os dois.

| Teste | O que observar |
| --- | --- |
| Operações básicas | Criar, apagar, assinar e cancelar; repetição de pedido; aviso quando não há inscritos. |
| Carga normal | Tempo de entrega e quantidade de mensagens entregues por segundo. |
| Consumidor lento | Fila limitada, redução do ritmo para esse consumidor, descartes e impacto nos demais. |
| Publicador excessivo | Respeito ao teto individual e ao limite global; recusas no broker se o cliente ignorar a regra. |
| Ajuste dinâmico | Taxa estável sem dificuldade, redução sob pressão e recuperação gradual até o teto. |
| Dois brokers | Publicador em um e consumidor no outro; repasse sem ciclos. |
| Falha e perda de comunicação | Tempo sem entregas, troca de broker, reconstrução das inscrições e pedidos sem confirmação. |

### Resultados a registrar

Latência: tempo até o consumo da mensagem. No gerador, usaremos o mesmo relógio do processo para medir do instante programado de envio até o consumo. Vazão: entregas por segundo. Também serão registradas entregas ausentes, duplicatas, recusas, descartes, tamanho das filas e evolução da taxa. Ausência não identifica sozinha a causa de perda.

Cada teste será repetido com a mesma configuração. Vamos registrar hardware, tamanho das mensagens, clientes, duração, CPU e memória do broker. Esses dados serão apresentados em gráficos e explicados no relatório final. As metas numéricas serão combinadas antes das medições.

### Divisão do trabalho

Felipe e Maria Luiza: dividir entre si o broker e a biblioteca cliente/transporte, conforme o acordo da equipe. José: limitador, regra de ajuste de taxa, gerador de carga, métricas, gráficos e análise, em conjunto com quem integra esses mecanismos ao broker e ao cliente.

### Escopo da versão reduzida

Sem página web, histórico de publicações, autenticação de produção, coordenação por consenso ou otimizações avançadas de memória. A base atual de ferramentas será reaproveitada onde possível; UDP, JSON, identificação e ajuste dinâmico exigem adaptação do código existente.

Referências: enunciado Projeto - Middleware Publish/Subscribe, Prof. Ruan Delgado Gomes; RFC 768 (UDP), rfc-editor.org/rfc/rfc768; RFC 8085 (uso de UDP), rfc-editor.org/rfc/rfc8085; RFC 8259 (JSON), rfc-editor.org/rfc/rfc8259. Os RFCs fundamentam as características dos protocolos; as regras de filas, ajuste e repasse acima são propostas da equipe para avaliação.
