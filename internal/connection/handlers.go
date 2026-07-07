package connection

import (
	"bufio"
	"log"
	"net"
	"strings"
	"time"

	"github.com/andresborn/chat-server-go/internal/chatroom"
	"github.com/andresborn/chat-server-go/internal/models"
)

func HandleConnection(conn net.Conn, cr *chatroom.Chatroom) {

	id := getId(conn)
	client := &models.Client{Conn: conn, ID: id, Outgoing: make(chan models.Message, 16)}

	defer func() {
		log.Printf("Closing connection with %s\n", conn.RemoteAddr().String())
		client.Conn.Close()
	}()

	log.Printf("Client connection: %s\n", conn.RemoteAddr().String())

	cr.Subscribe <- client

	go handleWrite(client)

	handleRead(client, cr) // Blocks until client disconnect returns scanner error

	// handleUnsub closes channel. cr.handleWrite finishes sending queued messages (which will most
	// likely return an error), exits, unsubscribes, and finally connection closes on defer.
	cr.Unsubscribe <- client

}

func handleWrite(c *models.Client) {
	for message := range c.Outgoing {
		err := sendMessage(c.Conn, message)
		if err != nil {
			log.Println("Error sending message: ", err)
			return
		}
	}
}

func handleRead(c *models.Client, cr *chatroom.Chatroom) {
	scanner := bufio.NewScanner(c.Conn)

	for scanner.Scan() {
		message := models.Message{From: c.ID, Text: scanner.Text()}
		cr.Broadcast <- message
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error from %s: %v ", c.Conn.RemoteAddr().String(), err)
		return
	}
}

func sendMessage(conn net.Conn, message models.Message) error {
	err := conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		log.Println(err)
		return err
	}
	_, err = conn.Write([]byte(message.From + ": " + message.Text + "\n"))
	if err != nil {
		log.Println("Error sending message: ", err)
		return err
	}
	return nil
}

func getId(conn net.Conn) string {
	id := strings.Split(conn.RemoteAddr().String(), ":")[1]
	return id
}
