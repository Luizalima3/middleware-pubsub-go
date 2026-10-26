# Biblioteca Cliente Pub/Sub (Cenário C)

Esta biblioteca fornece uma interface simplificada para interação com o broker de mensagens Pub/Sub, otimizada para baixa latência e alta disponibilidade.

## Funcionalidades

- **Conexão Resiliente**: Suporta múltiplos brokers com failover automático e reconexão via Backoff Exponencial com Jitter.
- **Gestão de Estado**: Reinscrição automática em tópicos após reconexão.
- **Protocolo Binário**: Comunicação eficiente via WebSockets.

## Como usar

### Conectar
```go
urls := []string{"ws://broker1:8080", "ws://broker2:8080"}
client, err := client.Conectar(ctx, urls)
```

### Publicar e Assinar
```go
// Publicar mensagem
id, err := client.Publicar(ctx, "leilao/lances", []byte("Lance: R$ 100"))

// Assinar tópico
_, err = client.Assinar(ctx, "leilao/lances")

// Remover inscrição
_, err = client.Cancelar(ctx, "leilao/lances")
```

### Gerenciar Tópicos
```go
client.CriarTopico(ctx, "novo/topico")
client.ApagarTopico(ctx, "velho/topico")
```

### Receber Mensagens
As mensagens chegam de forma assíncrona através do canal `Eventos()`:
```go
for msg := range client.Eventos() {
    if msg.Tipo == protocol.TipoSemInscritos {
        fmt.Printf("Aviso: Mensagem %d descartada (sem inscritos)\n", msg.ID)
    } else {
        fmt.Printf("Mensagem no tópico %s: %s\n", msg.Topico, string(msg.Conteudo))
    }
}
```
