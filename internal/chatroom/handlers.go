package chatroom

import (
	"log"

	"github.com/andresborn/chat-server-go/internal/models"
)

func Init() *Chatroom {
	room := Chatroom{
		Join:        make(chan *models.Client),
		Leave:       make(chan *models.Client),
		Broadcast:   make(chan models.Message),
		Private:     make(chan models.Message),
		Subscribe:   make(chan models.Message),
		Unsubscribe: make(chan models.Message),
		Topic:       make(chan models.Message),
		clients:     map[string]*models.Client{},
		topics:      map[string]*models.Topic{},
	}
	return &room
}

// Define chatroom methods
func (cr *Chatroom) handleBroadcast(message models.Message) {
	clients := make([]*models.Client, 0) // Local copy
	cr.mu.Lock()
	for _, client := range cr.clients {
		if client.Username == message.From {
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
			log.Println("Message dropped for slow client in broadcast: ", client.Username)
		}
	}
}

func (cr *Chatroom) handlePrivate(message models.Message) {
	cr.mu.Lock()
	if client, ok := cr.clients[message.To]; ok {
		client.Outgoing <- models.Message{From: message.From, Text: message.Text}
	} else {
		cr.clients[message.From].Outgoing <- models.Message{From: "Server", Text: "User not found."}
	}
	cr.mu.Unlock()
}

func (cr *Chatroom) handleTopic(message models.Message) {

	cr.mu.Lock()
	defer cr.mu.Unlock()

	var topicSubs []string // Local copy
	if topic, ok := cr.topics[message.Topic]; ok {
		topicSubs = topic.Subscribers
	} else {
		// Write to sender that topic didn't exist but was created
		// Create topic if it doesn't exist
		cr.topics[message.Topic] = &models.Topic{Name: message.Topic, Subscribers: make([]string, 0)}
	}

	var clients []*models.Client
	for _, sub := range topicSubs {
		clients = append(clients, cr.clients[sub])
	}

	for _, client := range clients {
		select {
		case client.Outgoing <- message:
		default:
			log.Println("Message dropped for slow client in topic: ", client.Username)
		}
	}

}

func (cr *Chatroom) handleTopicSubscribe(message models.Message) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if topic, ok := cr.topics[message.Topic]; ok {
		topic.Subscribers = append(topic.Subscribers, message.From)
	} else {
		// Write to client that topic didn't exist but was created
		// Create topic
		cr.topics[message.Topic] = &models.Topic{Name: message.Topic, Subscribers: make([]string, 0)}
		cr.topics[message.Topic].Subscribers = append(cr.topics[message.Topic].Subscribers, message.From)
	}

}

func (cr *Chatroom) handleTopicUnsubscribe(message models.Message) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if topic, ok := cr.topics[message.Topic]; ok {
		updatedSubs := make([]string, len(topic.Subscribers)-1)
		for i := range topic.Subscribers {
			if topic.Subscribers[i] != message.From {
				updatedSubs = append(updatedSubs, topic.Subscribers[i])
			}
		}
		topic.Subscribers = updatedSubs
	} else {
		// Write to client that topic doesn't exist, no unsubscribe needed
	}
}

func (cr *Chatroom) handleJoin(client *models.Client) {
	cr.mu.Lock()
	cr.clients[client.Username] = client
	cr.mu.Unlock()
}

func (cr *Chatroom) handleLeave(client *models.Client) {
	cr.mu.Lock()
	delete(cr.clients, client.Username)
	cr.mu.Unlock()

	// Close channel safely
	select {
	case <-client.Outgoing:
		// Already closed
	default:
		close(client.Outgoing)
	}
}

func (cr *Chatroom) ClientExists(id string) bool {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	if _, ok := cr.clients[id]; ok {
		return true
	}
	return false

}
