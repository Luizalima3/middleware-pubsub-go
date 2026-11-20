# Controle de carga e avaliação UDP

## O que foi adaptado

O gerador usa agora a mesma biblioteca UDP/JSON da aplicação. Ele espera a confirmação de criação e inscrições, inicia uma agenda fixa e separa a geração de trabalho das escritas por uma fila limitada. Se não acompanhar a agenda, conta publicações puladas em vez de crescer sem limite.

A regra adaptativa está em internal/controle/adaptativo.go. O bucket controla taxa/rajada e pode mudar a reposição sem renovar as fichas. O cliente aplica a espera local; o broker confere a cota por identificador e os limites globais. Só contar com o cliente não protege contra um emissor que ignora a regra.

## Experimentos

Execute os comandos do README. Para comparar resultados, use o mesmo hardware, payload, quantidade de usuários, duração e configuração do broker; repita cada cenário. Use tópicos e pastas de saída diferentes.

O broker remove sessões inativas por expiração. Entre experimentos que devam ter condições iniciais iguais, reinicie os brokers ou aguarde a expiração das sessões/interesses. Tópicos persistem em memória até exclusão ou reinício, respeitando o limite de quantidade.

O primeiro anúncio de interesse entre brokers é assíncrono. Para avaliar falha, use duas instâncias com -par configurado em ambos os lados e informe as duas URLs aos clientes. A opção -distribuir alterna a ordem de preferência para testar publicador e consumidor em instâncias distintas.

- Normal: carga dentro das cotas; registrar linha de base.
- Lento: atrasar a leitura da aplicação (-lentos, -atraso-lento), reduzir -buffer-cliente e manter carga suficiente por tempo suficiente.
- Excesso: -ignorar-controle simula um publicador que não respeita o ritmo local; o broker deve recusar o excedente.
- Ajuste: observar taxa.svg e a coluna de taxa no CSV, junto de filas e contadores. A taxa permanece no teto se não houver pressão; recupera gradualmente apenas quando há demanda.
- Dois brokers: distribuição remota sem repasse circular.
- Falha: interromper uma instância durante a carga e observar a recuperação; este relatório não extrai automaticamente a duração exata de cada indisponibilidade.
- Comandos: criar/apagar, assinar/cancelar, repetição e resposta perdida são cobertos pelos testes de integração.

## Como interpretar

- Latência: instante programado no gerador até o consumo na aplicação. Inclui atraso da geração, espera pelo controle de taxa, filas e atraso artificial de um consumidor lento.
- O relógio é o mesmo processo. O payload de teste usa metadados ASCII; não calcula latência subtraindo relógios de máquinas diferentes.
- p50/p95/p99 são aproximações de um histograma de memória fixa, com limites de 2%. Zero com nenhuma entrega não é sucesso de latência.
- Vazão de publicações: escritas UDP sem erro pela duração configurada. Vazão de entregas: entregas únicas pela janela observada, incluindo drenagem.
- Escrita UDP sem erro não confirma aceite do broker. Envios com erro ficam fora do denominador de entregas ausentes, pois podem ter sido parcialmente processados.
- Ausentes: pares publicação/assinante não observados até o fim da drenagem. Podem incluir rejeição, descarte, perda de rede, falha ou atraso.
- Avisos RATE_LIMIT e DESCARTADAS podem ser incompletos se a sinalização falhar. O resumo separa descartes avisados pelo broker e descartes na fila de leitura do cliente.
- A taxa média é amostrada nos instantes oferecidos pelo agendador; não equivale à vazão. Intervalos sem amostras são identificados no CSV.
- CPU e memória do broker devem ser medidos no processo/container do servidor, separadamente do gerador.

## Arquivos de cada execução

- resumo.json: configuração, contadores, percentis e taxa final média.
- intervalos.csv: contagens por segundo, p99 e taxa média com número de amostras.
- vazao.svg, latencia.svg e taxa.svg: gráficos vetoriais.
- relatorio.html: visualização local que abre no navegador; não é um cliente web.

O gerador limita sua própria memória: até 1 milhão de publicações, 100 milhões de pares de entrega, 10 minutos de duração e 1 minuto de drenagem. O payload de teste deve ter de 96 a 512 bytes. Isso permite encaixar os metadados legíveis e os campos do protocolo no datagrama de 1.000 bytes.

## Limitações

Este é um transporte de melhor esforço para rede local controlada. Sem autenticação, identidades declaradas não impedem fraude. Contadores agregados ajudam a perceber falta de progresso/perdas repetidas, mas não localizam a causa de cada ausência. O feedback também pode se perder.

Os testes locais verificam o comportamento implementado. Resultados de capacidade exigem experimentos no hardware/rede da apresentação e metas acordadas com a equipe/professor.
