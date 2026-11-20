# Adequação ao documento simplificado v3

Referência: Documento_Projeto_PubSub_Cenario_C_v3_simplificado.pdf, revisão de 05/11/2026.

## Requisitos mínimos

| Requisito | Implementação | Verificação |
| --- | --- | --- |
| Mensagem por tópico | protocol.Mensagem, broker.publicar | Publicação/recebimento por tópico. |
| Publicar, assinar e cancelar | client.Publicar, Assinar, Cancelar | TestPublicarAssinarCancelarApagarENotificar. |
| Criar e apagar em execução | Comandos administrativos confirmados | Teste básico e TestPedidoRepetidoNaoApagaTopicoRecriado. |
| Descartar sem inscritos e avisar | SEM_INSCRITOS com repetição e RECEBIDO | TestAvisoSemInscritosRepeteAteConfirmacao. |
| Encaminhar aos inscritos e liberar | Fila por tópico e por consumidor; envio/descarte libera referência | Teste básico, lento e carga com fan-out. |
| Absorver rajadas | Fila de tópico limitada por itens/bytes | Testes de fila e consumidor lento; aviso quando a entrada enche. |
| Dois brokers transparentes ao cliente | Par fixo, interesses, repasse em um salto e reinscrição | TestDoisBrokersRepasseSemCicloETopicos e TestFailoverReenviaInscricoes. |

## Decisões novas

- Transporte UDP/JSON no lugar de WebSocket/binário.
- Datagrama completo de até 1.000 bytes, sem fragmentação na aplicação.
- Cliente e protocolo públicos; biblioteca utilizável por outros programas Go.
- Comandos e avisos administrativos com confirmação limitada; publicações sem reenvio.
- Taxa adaptativa com teto, preservando a taxa sem pressão e sem recuperação ociosa.
- Redução pela metade; recuperação de 10% após duas avaliações boas com demanda.
- Limites por identificador no broker, globais de entrada e saída e específicos para controle/par.
- Filas e tabelas de IDs limitadas; sessão/interesse expiram.
- Dois brokers com repasse direto; uma publicação remota não é repassada novamente.
- Aplicação no terminal e relatórios locais, sem página web.

## Detalhes concretizados nesta implementação

Valores padrão e o formato dos campos estão no PROTOCOL.md e no help dos comandos. As filas antigas de eco e a dependência WebSocket foram retiradas. Histogramas e exportadores da avaliação anterior foram reaproveitados.

Os interesses são renovados por tópico e expiram. Uma fila interna por par não descarta silenciosamente sem contar: a falta de espaço registra descarte. O limite global de entrada conta publicações locais e repasses; o de saída conta todos os datagramas. A vazão sustentada pode ter uma rajada inicial explicitamente limitada.

A identificação é estável durante a vida do cliente e declarada pelo usuário. Uma reinicialização do cliente com ID automático gera outro ID. O controle por participante não é uma identidade autenticada nem um limite global sincronizado entre os dois brokers.

## O que não é prometido

- Entrega confiável de cada publicação, ausência de interrupções ou ordem global.
- Consistência imediata de tópicos em partições ou administração concorrente.
- Proteção contra fraude de identidade e uso seguro em Internet aberta.
- Capacidade de produção obtida apenas com testes locais.
- Ciência de todos os avisos quando todas as tentativas de sinalização falham.

A interpretação do requisito de encaminhamento como tentativa de envio, com perda de UDP e descarte na sobrecarga, segue sendo uma decisão a alinhar com o professor.
