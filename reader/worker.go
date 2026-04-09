package reader

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	amqppool "github.com/ttrtcixy/go-amqp-mngr"
)

type WorkerConfig struct {
	amqppool.TopologyConfig

	SetupChannelEachUpdate bool          `env:"AMQP_SETUP_CHANNEL_EACH_UPDATE,required"`
	MessageCount           int           `env:"AMQP_CHANNEL_MESSAGE_COUNT,required"`
	SendMessageTimeout     time.Duration `env:"AMQP_SETUP_SEND_MESSAGE_TIMEOUT,required"`
	MaxRetries             int           `env:"AMQP_MAX_RETRIES,required"`
}

type MessageConsumer interface {
	Send(ctx context.Context, body []byte) error
	Close(ctx context.Context) error
}

type worker struct {
	cfg *WorkerConfig
	log *slog.Logger

	// for soft shutdown, if channel closed, the worker must begin shutting down after reading the messages
	stopCh   chan struct{}
	consumer MessageConsumer

	ch *amqp.Channel

	queueName string
	//q    amqp.Queue
	msgs <-chan amqp.Delivery

	stateCh chan *amqp.Error
}

func newWorker(cfg *WorkerConfig, log *slog.Logger, mc MessageConsumer, stopCh chan struct{}) *worker { // todo mb ptr?
	return &worker{cfg: cfg, log: log, consumer: mc, stopCh: stopCh}
}

func (wk *worker) SetTopology() error {
	return wk.setupTopology()
}

func (wk *worker) setupTopology() (err error) {
	const op = "reader.setupTopology"

	if err := amqppool.SetupTopology(wk.ch, &wk.cfg.TopologyConfig); err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	return nil
}

func (wk *worker) configureChannel() (err error) {
	const op = "reader.configureChannel"

	wk.queueName = wk.cfg.QueueName

	if err = wk.ch.Qos(wk.cfg.MessageCount, 0, false); err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	msgs, err := wk.ch.Consume(wk.queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	wk.msgs = msgs

	return nil
}

func (wk *worker) close() {
	if wk.ch != nil {
		_ = wk.ch.Close()
		wk.ch = nil
	}
}

func (wk *worker) Close(ctx context.Context) error {
	var err error

	err = wk.consumer.Close(ctx)

	wk.close()

	if err != nil {
		return err
	}
	return nil
}

func (wk *worker) IsInvalid() bool {
	if wk.ch == nil || wk.ch.IsClosed() {
		return true
	}

	select {
	case <-wk.stateCh:
		return true
	default:
		return false
	}
}

func (wk *worker) Do(ctx context.Context) (err error) {
	for {
		select {
		case msg, ok := <-wk.msgs:
			if !ok { // if the channel is closed
				wk.close()

				select {
				case <-wk.stopCh:
					return amqppool.ErrClosed
				default:
					return amqppool.ErrNeedReconnect
				}
			}

			if err := wk.processMessage(ctx, msg); err != nil {
				return err
			}
		case <-wk.stopCh:
			_ = wk.ch.Cancel("", true)

			wk.close()

			return amqppool.ErrClosed
		case <-ctx.Done():

			wk.close()

			return amqppool.ErrCtxClose
		}
	}
}

// DoSoft - an analogue of a simple Do, only when receiving a completion signal, it tries to read from the queue.
// Deprecated: more complex logic that can create problems and hide bugs, especially if there are connection problems.
func (wk *worker) DoSoft(ctx context.Context) (err error) {

	stopSig := wk.stopCh

	for {
		select {
		case msg, ok := <-wk.msgs:
			if !ok { // if the channel is closed
				select {
				case _, alive := <-wk.stopCh:
					if !alive {
						return amqppool.ErrClosed
					}
					return amqppool.ErrNeedReconnect
				default:
					wk.close()

					return amqppool.ErrNeedReconnect
				}
			}

			if err := wk.processMessage(ctx, msg); err != nil {
				return err
			}
		case <-stopSig:
			if wk.ch != nil {
				if err := wk.ch.Cancel("", false); err != nil {

					wk.close()

					return amqppool.ErrNeedReconnect
				}

				stopSig = nil
			}

		case <-ctx.Done():

			wk.close()

			return amqppool.ErrCtxClose
		}
	}
}

func (wk *worker) processMessage(ctx context.Context, msg amqp.Delivery) (err error) {
	sendCtx, cancel := wk.withTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	if err = wk.consumer.Send(sendCtx, msg.Body); err != nil {
		return wk.handleSendErr(ctx, msg, err)
	}

	if err = msg.Ack(false); err != nil {
		// If Ack fails, the connection with the broker is lost. You will also not process the next message.
		wk.close()

		return amqppool.ErrNeedReconnect
	}

	return nil
}

func (wk *worker) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if wk.cfg.SendMessageTimeout > 0 {
		return context.WithTimeout(ctx, wk.cfg.SendMessageTimeout)
	}

	return ctx, nil
}

func (wk *worker) handleSendErr(ctx context.Context, msg amqp.Delivery, err error) error {
	if wk.getRetryCount(msg.Headers) > wk.cfg.MaxRetries {
		// to many attempts to send message
		wk.log.LogAttrs(
			ctx,
			slog.LevelError,
			"too many attempts to resend message, message deleted",
			slog.String("error", err.Error()),
		)
		// stop trying to send a message by confirming it
		if err := msg.Ack(false); err != nil {
			wk.close()

			return amqppool.ErrNeedReconnect
		}

		return nil
	}
	// if an error occurred while sending and the number of attempts to send the message does not exceed the maximum
	if err := msg.Nack(false, false); err != nil { // send them to DLX, from there they will go back to the queue
		wk.close()

		return amqppool.ErrNeedReconnect
	}

	wk.log.LogAttrs(ctx, slog.LevelError, "error sending message", slog.String("error", err.Error()))

	return nil
}

func (wk *worker) getRetryCount(headers amqp.Table) int {
	val, ok := headers["x-death"]
	if !ok {
		return 0
	}

	deathList, ok := val.([]interface{})
	if !ok || len(deathList) == 0 {
		return 0
	}

	if lastDeath, ok := deathList[0].(amqp.Table); ok {
		if count, ok := lastDeath["count"].(int64); ok {
			return int(count)
		}
	}

	return 0
}

func (wk *worker) SetChannel(ch *amqp.Channel) (err error) {
	const op = "reader.SetChannel"

	wk.ch = ch

	wk.stateCh = ch.NotifyClose(make(chan *amqp.Error, 1))

	if wk.cfg.SetupChannelEachUpdate {
		if err = wk.setupTopology(); err != nil {
			return fmt.Errorf("%s - declare channel topology error -> %w", op, err)
		}
	}

	// configure channel
	if err = wk.configureChannel(); err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	return err
}
