package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

func main() {
	http.HandleFunc("/ws", tratarConexao)
	log.Println("broker falso ouvindo em ws://localhost:8080/ws")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func tratarConexao(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Println("erro ao aceitar conexão:", err)
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(64 * 1024)
	log.Println("cliente conectado")

	for {
		tipo, dados, err := conn.Read(r.Context())
		if err != nil {
			log.Println("cliente saiu:", err)
			return
		}
		log.Printf("recebi %d bytes", len(dados))

		ctx, cancelar := context.WithTimeout(r.Context(), 5*time.Second)
		err = conn.Write(ctx, tipo, dados)
		cancelar()
		if err != nil {
			log.Println("erro ao enviar:", err)
			return
		}
	}
}
