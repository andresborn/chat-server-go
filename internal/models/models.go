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
	ID       string
	Outgoing chan Message // Client inbox. Server flushes out these messages to the client.
}
