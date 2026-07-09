package chatroom

func (cr *Chatroom) Run() {
	go func() {
		for {
			select {
			case client := <-cr.Join:
				cr.handleJoin(client)
			case client := <-cr.Leave:
				cr.handleLeave(client)
			case message := <-cr.Broadcast:
				cr.handleBroadcast(message)
			case message := <-cr.Private:
				cr.handlePrivate(message)
			case message := <-cr.Topic:
				cr.handleTopic(message)
			case message := <-cr.Subscribe:
				cr.handleTopicSubscribe(message)
			case message := <-cr.Unsubscribe:
				cr.handleTopicUnsubscribe(message)
			case message := <-cr.Commands:
				cr.handleCommand(message)
			}

		}
	}()
}
