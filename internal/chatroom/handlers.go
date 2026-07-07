package chatroom

import (
	"log"

	"github.com/andresborn/chat-server-go/internal/models"
)

func Init() *Chatroom {
	room := Chatroom{
		Subscribe:   make(chan *models.Client),
		Unsubscribe: make(chan *models.Client),
		Broadcast:   make(chan models.Message),
		clients:     map[string]*models.Client{},
	}
	return &room
}

// Define chatroom methods
func (cr *Chatroom) handleBroadcast(message models.Message) {
	clients := make([]*models.Client, 0) // Local copy
	cr.mu.Lock()
	for _, client := range cr.clients {
		if client.ID == message.From {
			continue
		}
		clients = append(clients, client)

	}
	cr.mu.Unlock()

	log.Printf("Broadcasting %s from %s\n", message.Text, message.From)

	for _, client := range clients {
		select {
		case client.Outgoing <- message:
		default:
			log.Println("Message dropped for slow client: ", client.ID)
		}
	}
}

func (cr *Chatroom) handleSub(client *models.Client) {
	cr.mu.Lock()
	cr.clients[client.ID] = client
	cr.mu.Unlock()
}

func (cr *Chatroom) handleUnsub(client *models.Client) {
	cr.mu.Lock()
	delete(cr.clients, client.ID)
	cr.mu.Unlock()

	// Close channel safely
	select {
	case <-client.Outgoing:
		// Already closed
	default:
		close(client.Outgoing)
	}
}
