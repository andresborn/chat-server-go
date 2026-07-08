package chatroom

import (
	"sync"

	"github.com/andresborn/chat-server-go/internal/models"
)

type Chatroom struct {
	// Channels
	Subscribe   chan *models.Client
	Unsubscribe chan *models.Client
	Broadcast   chan models.Message
	Private     chan models.PrivateMessage

	// State
	clients map[string]*models.Client
	mu      sync.Mutex
}
