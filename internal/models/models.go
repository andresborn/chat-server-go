package models

import (
	"net"
)

type Message struct {
	From    string
	To      string
	Text    string
	Topic   string
	Command string // users or topics
}

type Client struct {
	Conn     net.Conn
	Username string
	Outgoing chan Message // Client inbox. Server flushes out these messages to the client.
}

type Topic struct {
	Subscribers []string // usernames
	Name        string
}
