# Biblioteca cliente UDP

Pacote público: github.com/Luizalima3/middleware-pubsub-go/client.

```go
ctx, cancelar := context.WithTimeout(context.Background(), 5*time.Second)
defer cancelar()
c, err := client.Conectar(ctx, []string{"127.0.0.1:8080", "127.0.0.1:8081"})
if err != nil { return err }
defer c.Fechar()

_, err = c.Assinar(ctx, "jogo/reacoes") // aguarda OK e cria tópico se faltar
if err != nil { return err }
_, err = c.Publicar(ctx, "jogo/reacoes", []byte("Gol!"))
if err != nil { return err }
for m := range c.Eventos() {
    fmt.Printf("%s %s: %s\n", m.Tipo, m.Topico, m.Conteudo)
}
```

Conectar usa o contexto apenas para a abertura inicial. Depois de conectar, a vida do cliente termina com Fechar. O exemplo precisa de imports context, time, fmt e o pacote client; para um programa completo, consulte cmd/demo.

## Configuração

ConectarComConfig aceita Brokers (um ou dois), ID, TaxaMaxima, Buffer, Intervalo, Timeout, Repeticao e Tentativas. Valores padrão: taxa 100 publicações/s, fila de dados 32, atividade 1 s, timeout 3 s, repetição 250 ms e três tentativas.

O ID vazio é gerado aleatoriamente. O mesmo ID em vários endpoints compartilha o bucket de admissão naquele broker. Ele é declarado, sem autenticação. Prefira um ID por instância de aplicação.

## Operações

CriarTopico, ApagarTopico, Assinar e Cancelar são síncronas em relação à confirmação. Repetem com o mesmo ID até o limite e retornam erro se a confirmação faltar. Nesse caso a operação pode ter sido executada; o erro expressa resultado desconhecido. A biblioteca serializa os comandos administrativos.

Publicar aplica o ritmo local, aceita contexto/prazo e retorna depois da escrita UDP. Não é uma confirmação de aceite/entrega. Conteúdo deve ser UTF-8 e caber no datagrama JSON completo. Use protocol.Codificar para inspecionar o formato.

Eventos retorna entregas e avisos; respostas administrativas também podem ser expostas. A fila de leitura dos dados é limitada e descarta os mais antigos quando necessário. Avisos importantes possuem fila separada e confirmação. A aplicação precisa continuar consumindo Eventos.

Taxa informa a taxa de controle atual; BrokerAtual informa o endereço disponível ou vazio durante recuperação; Descartadas conta descartes na fila local; Estatisticas solicita os contadores do broker via UDP.

## Recuperação

Falta de comunicação faz a biblioteca tentar outro endereço, com espera crescente e aleatória entre rodadas que falham, e reenviar as inscrições antes de liberar novas publicações. Durante a troca, Publicar devolve ErrDesconectado. O tópico é criado implicitamente por ASSINAR no destino.

IDs recentes limitam duplicatas, não garantem execução única para sempre. Dados e inscrições não são persistidos. Clientes fora da rede local ou sem confiar na origem exigem um projeto adicional de autenticação, rede e congestionamento.
