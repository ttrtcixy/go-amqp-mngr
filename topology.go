package amqppool

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TopologyConfig struct {
	QueueName         string        `env:"AMQP_QUEUE_NAME,required"`
	QueueRetryName    string        `env:"AMQP_QUEUE_RETRY_NAME,required"`
	QueueRetryTTL     time.Duration `env:"AMQP_QUEUE_TTL"                    envDefault:"1m"`
	ExchangeName      string        `env:"AMQP_EXCHANGE_NAME,required"`
	RetryExchangeName string        `env:"AMQP_RETRY_EXCHANGE_NAME,required"`
	RouteKey          string        `env:"AMQP_ROUTE_KEY,required"`
}

func SetupTopology(ch *amqp.Channel, cfg *TopologyConfig) (err error) {
	const op = "emailsender.setupTopology"

	// main_exchange
	if err = ch.ExchangeDeclare(
		cfg.ExchangeName,
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("%s - exchange declare error -> %w", op, err)
	}

	// retry_exchange
	if err = ch.ExchangeDeclare(
		cfg.RetryExchangeName,
		amqp.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("%s - exchange declare error -> %w", op, err)
	}

	// main queue
	mainArgs := amqp.Table{
		"x-dead-letter-exchange": cfg.RetryExchangeName,
	}

	if _, err = ch.QueueDeclare(
		cfg.QueueName,
		true,
		false,
		false,
		false,
		mainArgs,
	); err != nil {
		return fmt.Errorf("%s - queue declare error -> %w", op, err)
	}

	if err = ch.QueueBind(cfg.QueueName, cfg.RouteKey, cfg.ExchangeName, false, nil); err != nil {
		return fmt.Errorf("%s - queue bind error -> %w", op, err)
	}

	// retry queue
	retryArgs := amqp.Table{
		"x-dead-letter-exchange":    cfg.ExchangeName,
		"x-dead-letter-routing-key": cfg.RouteKey,
		"x-message-ttl":             int32(cfg.QueueRetryTTL.Milliseconds()),
	}
	if _, err = ch.QueueDeclare(cfg.QueueRetryName, true, false, false, false, retryArgs); err != nil {
		return fmt.Errorf("%s - queue declare error -> %w", op, err)
	}

	if err = ch.QueueBind(cfg.QueueRetryName, cfg.RouteKey, cfg.RetryExchangeName, false, nil); err != nil {
		return fmt.Errorf("%s - queue bind error -> %w", op, err)
	}

	return nil
}
