package models

import (
	"net"
)

type Message struct {
	From string
	Text string
}

type Client struct {
	Conn     net.Conn
	Username string
	Outgoing chan Message // Client inbox. Server flushes out these messages to the client.
}

type PrivateMessage struct {
	From string
	Text string
	To   string
}
