package chatroom

import (
	"sync"

	"github.com/andresborn/chat-server-go/internal/models"
)

type Chatroom struct {
	// Join/Leave chatroom
	Join  chan *models.Client
	Leave chan *models.Client

	// Channels
	Broadcast chan models.Message
	Private   chan models.Message
	Topic     chan models.Message

	// Topic Subscribe/Unsubscribe channels
	Subscribe   chan models.Message
	Unsubscribe chan models.Message

	// Commands channel
	Commands chan models.Message

	// State
	clients map[string]*models.Client
	topics  map[string]*models.Topic
	mu      sync.Mutex
}
