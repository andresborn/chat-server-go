# chat-server-go
Chat Server written in Go

This project build on top of this [previous version](https://github.com/andresborn/net-fundamentals/tree/main/chat-server).

For this chat server we will build private messaging, Pub/Sub topics and gRPC endpoints. 
We'll also build a Go reverse-proxy for load balancing and we'll deploy 3 replicas using Docker compose.

## TODO
- [X] Verify that client exists when sending to topic subscribers. If it doesn't exist, remove.
- [X] Fix: sending message to topic after unsubscribing from topic throws panic. Reason: unsafe access to client map.
- [X] Verify usernames: reject empty, reject special characters, reject more than 20 character names
- [X] Create `/users` command to list connected users
- [X] Create `/topics` command to list available topics and sub count
- [ ] Fix: Blank topics are being created.