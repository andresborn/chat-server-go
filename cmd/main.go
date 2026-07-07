package main

import (
	"errors"
	"log"
	"net"

	"github.com/andresborn/chat-server-go/internal/chatroom"
	"github.com/andresborn/chat-server-go/internal/connection"
)

func main() {
	host, port := "127.0.0.1", "8080"
	addr := net.JoinHostPort(host, port)
	listener, err := net.Listen("tcp", addr)

	if err != nil {
		log.Fatal("Error starting server: ", err)
		return
	}

	defer listener.Close()

	log.Printf("Listening on %s", addr)

	cr := chatroom.Init()

	// Event broker
	cr.Run()

	for {
		conn, err := listener.Accept()

		if errors.Is(err, net.ErrClosed) {
			log.Println("Listener connection closed: ", err)
			return
		}

		if err != nil {
			log.Println("Error accepting connection: ", err)
			continue
		}
		go connection.HandleConnection(conn, cr)
	}
}
