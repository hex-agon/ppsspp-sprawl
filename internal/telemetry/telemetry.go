package telemetry

import "sprawl/internal/pspnet"

var telemetryCollector Collector

func Init(collector Collector) {
	telemetryCollector = collector
}

type ChatMessageEvent struct {
	MacAddr     pspnet.MacAddr `json:"mac_addr"`
	DisplayName string         `json:"display_name"`
	TitleId     string         `json:"title_id"`
	Message     []byte         `json:"message"`
}

func RecordChatMessage(message *ChatMessageEvent) {
	if telemetryCollector != nil {
		telemetryCollector.chatMessageEvent(message)
	}
}
