package chatroom

func (cr *Chatroom) Run() {
	go func() {
		for {
			select {
			case client := <-cr.Subscribe:
				cr.handleSub(client)
			case client := <-cr.Unsubscribe:
				cr.handleUnsub(client)
			case message := <-cr.Broadcast:
				cr.handleBroadcast(message)
			case privateMessage := <-cr.Private:
				cr.handlePrivate(privateMessage)
			}
		}
	}()
}
