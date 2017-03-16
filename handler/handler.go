package handler

import (
	"github.com/ndphu/espresso-commons/messaging"
	"github.com/ndphu/espresso-commons/repo"
)

type Handler interface {
	HandleMessage(msg *messaging.Message)
}

type CommandHandler struct {
	Handlers map[messaging.MessageType]Handler
}

func NewCommandHandler(dr *repo.DeviceRepo, tcr *repo.TextCommandRepo, gcr *repo.GPIOCommandRepo, r *messaging.MessageRouter) *CommandHandler {
	ch := &CommandHandler{}
	ch.Handlers = make(map[messaging.MessageType]Handler)
	ch.Handlers[messaging.GPIOCommandAdded] = NewGPIOCommandHandler(dr, gcr, r)
	ch.Handlers[messaging.TextCommandAdded] = NewTextCommandHandler(dr, tcr, r)
	return ch
}

func (t *CommandHandler) OnNewMessage(msg *messaging.Message) {
	handler, exists := t.Handlers[msg.Type]
	if exists {
		handler.HandleMessage(msg)
	}
}
