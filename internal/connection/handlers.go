package connection

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/andresborn/chat-server-go/internal/chatroom"
	"github.com/andresborn/chat-server-go/internal/models"
)

func HandleConnection(conn net.Conn, cr *chatroom.Chatroom) {
	log.Printf("Client connection: %s\n", conn.RemoteAddr().String())

	defer func() {
		log.Printf("Closing connection with %s\n", conn.RemoteAddr().String())
		conn.Close()
	}()

	// Prompt for username or reconnection
	conn.Write([]byte("Enter username: \n"))

	reader := bufio.NewReader(conn)
	input, err := reader.ReadString('\n')
	if err != nil {
		log.Println("Failed to read username:", err)
		return
	}
	input = strings.TrimSpace(input)

	if len(strings.Fields(input)) > 1 {
		conn.Write([]byte("Your username should have no spaces.\n"))
		return
	}

	if input == "" {
		conn.Write([]byte("Username can't be empty.\n"))
		return
	}

	if !utf8.ValidString(input) {
		conn.Write([]byte("Username needs to be valid UTF-8.\n"))
		return
	}

	if len(input) > 20 {
		conn.Write([]byte("Username can't be over 20 characters.\n"))
		return
	}

	if strings.HasPrefix(input, "/") {
		conn.Write([]byte("Username can't start with \"/\" \n"))
		return
	}

	if cr.ClientExists(input) {
		conn.Write([]byte("Username already in use, pick another one.\n"))
		return
	}

	conn.Write([]byte(fmt.Sprintf("Welcome to the chatroom %s. \n", input)))

	client := &models.Client{Conn: conn, Username: input, Outgoing: make(chan models.Message, 16)}

	cr.Join <- client

	go handleWrite(client)

	handleRead(client, cr) // Blocks until client disconnect returns scanner error

	// handleUnsub closes channel. cr.handleWrite finishes sending queued messages (which will most
	// likely return an error), exits, unsubscribes, and finally connection closes on defer.
	cr.Leave <- client

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
		text := scanner.Text()
		if strings.HasPrefix(text, "/") {
			handleMessage(c, cr, text)
			continue
		}
		// Broadcast
		cr.Broadcast <- models.Message{From: c.Username, Text: text}
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

func handleMessage(c *models.Client, cr *chatroom.Chatroom, fullText string) {
	split := strings.Split(fullText, " ")

	switch split[0] {
	case "/msg":
		if len(split) < 3 {
			c.Conn.Write([]byte("For private messages format should be: /msg <username> <text>. \n"))
			return
		}
		recipient := split[1]
		content := strings.Join(split[2:], " ")
		cr.Private <- models.Message{From: c.Username, To: recipient, Text: content}

	case "/topic": // Topic: /topic <topic> <text>
		if len(split) < 3 {
			c.Conn.Write([]byte("For topic messages format should be: /topic <topic> <text>. \n"))
			return
		}
		topic := split[1]
		content := strings.Join(split[2:], " ")
		cr.Topic <- models.Message{Topic: topic, Text: content, From: c.Username}

	case "/subscribe":
		if len(split) != 2 {
			c.Conn.Write([]byte("To subscribe to a topic type: /subscribe <topic-name>\n"))
			return
		}
		topic := split[1]
		cr.Subscribe <- models.Message{From: c.Username, Topic: topic}
	case "/unsubscribe":
		if len(split) != 2 {
			c.Conn.Write([]byte("To unsubscribe from a topic type: /unsubscribe <topic-name>\n"))
			return
		}
		topic := split[1]
		cr.Unsubscribe <- models.Message{From: c.Username, Topic: topic}
	case "/users":
		cr.Commands <- models.Message{From: c.Username, Command: "users"}
	case "/topics":
		cr.Commands <- models.Message{From: c.Username, Command: "topics"}
	default:
		c.Conn.Write([]byte(fmt.Sprintf("Not supported: %s \n", fullText)))
	}

}
