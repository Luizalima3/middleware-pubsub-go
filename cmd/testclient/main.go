package main

import (
	"context"
	"log"
	"time"

	"github.com/coder/websocket"
)

func main() {
	ctx, cancelar := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelar()

	conn, _, err := websocket.Dial(ctx, "ws://localhost:8080/ws", nil)
	if err != nil {
		log.Fatal("erro ao conectar: ", err)
	}
	defer conn.CloseNow()
	log.Println("conectado ao broker")

	err = conn.Write(ctx, websocket.MessageText, []byte("oi do cliente Go"))
	if err != nil {
		log.Fatal("erro ao enviar: ", err)
	}

	_, dados, err := conn.Read(ctx)
	if err != nil {
		log.Fatal("erro ao receber: ", err)
	}
	log.Printf("resposta: %s", dados)

	conn.Close(websocket.StatusNormalClosure, "tchau")
}
