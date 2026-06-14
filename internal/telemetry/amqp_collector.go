package telemetry

import (
	"context"
	"encoding/json"
	"math"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

const (
	amqpBufferSize     = 256
	amqpExchangeName   = "sprawl-telemetry"
	amqpReconnectBase  = time.Second
	amqpReconnectMax   = 30 * time.Second
	amqpPublishTimeout = 5 * time.Second

	queueChatMessage = "event.chat.message"
)

var amqpQueues = []string{
	queueChatMessage,
}

type AmqpCollector struct {
	url      string
	nodeName string
	events   chan *ChatMessageEvent
	done     chan struct{}
	cancel   context.CancelFunc
}

func NewAmqpCollector(url string, nodeName string) *AmqpCollector {
	ctx, cancel := context.WithCancel(context.Background())
	c := &AmqpCollector{
		url:      url,
		nodeName: nodeName,
		events:   make(chan *ChatMessageEvent, amqpBufferSize),
		done:     make(chan struct{}),
		cancel:   cancel,
	}
	go c.run(ctx)
	return c
}

func (c *AmqpCollector) chatMessageEvent(message *ChatMessageEvent) {
	select {
	case c.events <- message:
	default:
		log.Warn().Msg("telemetry: amqp buffer full, dropping event")
	}
}

func (c *AmqpCollector) Close() {
	c.cancel()
	<-c.done
}

func (c *AmqpCollector) run(ctx context.Context) {
	defer close(c.done)

	var attempt int
	for {
		conn, ch, err := c.connect()
		if err != nil {
			delay := backoffDelay(attempt)
			log.Warn().Err(err).Dur("retry_in", delay).Msg("telemetry: amqp connection failed")
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			attempt++
			continue
		}
		attempt = 0

		connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))

		if !c.publishLoop(ctx, ch, connClosed) {
			_ = ch.Close()
			_ = conn.Close()
			return
		}

		_ = ch.Close()
		_ = conn.Close()
		log.Info().Msg("telemetry: amqp connection lost, reconnecting")
	}
}

func (c *AmqpCollector) connect() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	err = ch.ExchangeDeclare(amqpExchangeName, "topic", true, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, err
	}

	for _, queue := range amqpQueues {
		q, err := ch.QueueDeclare(queue, true, false, false, false, nil)
		if err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return nil, nil, err
		}

		err = ch.QueueBind(q.Name, queue, amqpExchangeName, false, nil)
		if err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return nil, nil, err
		}
	}

	log.Info().Msg("telemetry: amqp connected")
	return conn, ch, nil
}

func (c *AmqpCollector) publishLoop(ctx context.Context, ch *amqp.Channel, connClosed chan *amqp.Error) (disconnected bool) {
	for {
		select {
		case <-ctx.Done():
			return false
		case <-connClosed:
			return true
		case evt := <-c.events:
			c.publish(ctx, ch, evt)
		}
	}
}

func (c *AmqpCollector) publish(ctx context.Context, ch *amqp.Channel, evt *ChatMessageEvent) {
	body, err := json.Marshal(evt)

	if err != nil {
		log.Error().Err(err).Msg("telemetry: failed to marshal event")
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, amqpPublishTimeout)
	defer cancel()

	err = ch.PublishWithContext(pubCtx, amqpExchangeName, queueChatMessage, false, false, amqp.Publishing{
		ContentType: "application/json",
		Headers: amqp.Table{
			"node": c.nodeName,
		},
		Body: body,
	})
	if err != nil {
		log.Error().Err(err).Msg("telemetry: failed to publish event")
	}
}

func backoffDelay(attempt int) time.Duration {
	delay := amqpReconnectBase * time.Duration(math.Pow(2, float64(attempt)))
	if delay > amqpReconnectMax {
		delay = amqpReconnectMax
	}
	return delay
}
