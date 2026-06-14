package telemetry

type Collector interface {
	chatMessageEvent(message *ChatMessageEvent)
}
