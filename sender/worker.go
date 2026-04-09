package sender

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	amqppool "github.com/ttrtcixy/go-amqp-mngr"
)

var (
	ErrNeedToRewrite = errors.New("need to rewrite msg")
)

type WorkerConfig struct {
	amqppool.TopologyConfig

	SetupChannelEachUpdate bool          `env:"SENDER_SETUP_CHANNEL_EACH_UPDATE,required"`
	MessageConfirmTimeout  time.Duration `env:"SENDER_CONFIRM_TIMEOUT,required"`
	DefaultMessageTTL      time.Duration `env:"SENDER_DEFAULT_MESSAGE_TTL"                envDefault:"1m"`
}

type worker struct {
	cfg *WorkerConfig
	log *slog.Logger

	storage *storage

	ch *amqp.Channel
	// !IMPORTANT library itself closes this channel when amqp.Channel is closed
	confirm chan amqp.Confirmation

	stateCh chan *amqp.Error
}

func newWorker(cfg *WorkerConfig, log *slog.Logger, st *storage) *worker {
	return &worker{cfg: cfg, log: log, storage: st}
}

func (wk *worker) SetChannel(ch *amqp.Channel) (err error) {
	const op = "sender.SetChannel"

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

func (wk *worker) SetTopology() error {
	return wk.setupTopology()
}

func (wk *worker) Close(_ context.Context) error {
	wk.close()

	return nil
}

func (wk *worker) close() {
	if wk.ch != nil {
		_ = wk.ch.Close()
		wk.ch = nil
	}
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

func (wk *worker) setupTopology() (err error) {
	const op = "sender.setupTopology"

	if err := amqppool.SetupTopology(wk.ch, &wk.cfg.TopologyConfig); err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	return nil
}

func (wk *worker) configureChannel() (err error) {
	const op = "sender.configureChannel"

	if err = wk.ch.Confirm(
		false,
	); err != nil {
		return fmt.Errorf("%s - add publishing confirmation error -> %w", op, err)
	}

	wk.confirm = wk.ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	return nil
}

func (wk *worker) Do(ctx context.Context) (err error) {
	const op = "sender.Do"

	for {
		m, ok := wk.storage.getMsg(ctx)
		if !ok { // Close channels or ctx.Done, stop worker
			return amqppool.ErrClosed
		}

		if err = wk.processMessage(ctx, m); err != nil {
			switch {
			case errors.Is(err, amqppool.ErrCtxClose),
				errors.Is(err, amqppool.ErrClosed),
				errors.Is(err, amqppool.ErrNeedReconnect):
				return err
			default:
				continue
			}
		}
	}
}

func (wk *worker) processMessage(ctx context.Context, m msg) (err error) {
	const op = "sender.processMessage"

	defer func() {
		if err != nil && !errors.Is(err, amqppool.ErrCtxClose) && !errors.Is(err, amqppool.ErrClosed) {
			wk.storage.pushInBacklog(ctx, m)
		}
	}()

	pubCtx, cancel := wk.withTimeout(ctx)
	if cancel != nil {
		defer cancel()
	}

	// todo контекст не работает, не поддерживается библиотекой.
	if err = wk.publish(pubCtx, wk.ch, m); err != nil {
		return err
	}

	select {
	case c, ok := <-wk.confirm:
		return wk.confirmMsg(c, ok, m.ctx)

	case <-pubCtx.Done():
		return wk.handleDone(ctx, pubCtx, m.ctx)
	}
}

func (wk *worker) confirmMsg(c amqp.Confirmation, ok bool, messageCtx context.Context) error {
	// if for some reason the confirmation channel is closed, you need to re-create the channel
	if !ok {
		wk.close()
		// since the channel is closing, we need to re-create it
		return amqppool.ErrNeedReconnect
	}
	// success
	if c.Ack {
		return nil
	}
	// If confirmation error.
	wk.log.LogAttrs(messageCtx, slog.LevelError, "message not confirmed")

	return ErrNeedToRewrite
}

func (wk *worker) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if wk.cfg.MessageConfirmTimeout > 0 {
		return context.WithTimeout(ctx, wk.cfg.MessageConfirmTimeout)
	}

	return ctx, nil
}

func (wk *worker) handleDone(rootCtx, pubCtx, messageCtx context.Context) error {
	// if there is a context error then terminate the application
	if rootCtx.Err() != nil {
		return amqppool.ErrCtxClose
	}
	// If confirmation timeout.
	if errors.Is(pubCtx.Err(), context.DeadlineExceeded) {
		wk.log.LogAttrs(messageCtx, slog.LevelWarn, "confirmation timeout, closing channel")
		// close because confirmation may come later
		wk.close()

		return amqppool.ErrNeedReconnect
	}

	return nil
}

func (wk *worker) publish(ctx context.Context, ch *amqp.Channel, m msg) error {
	if m.task.TTL == 0 {
		m.task.TTL = wk.cfg.DefaultMessageTTL
	}

	if err := ch.PublishWithContext(ctx, wk.cfg.ExchangeName, wk.cfg.RouteKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Expiration:   strconv.Itoa(int(m.task.TTL.Milliseconds())),
		Timestamp:    time.Now(),
		Body:         m.task.Body,
	}); err != nil {
		wk.log.LogAttrs(
			m.ctx,
			slog.LevelError,
			"error writing message to broker",
			slog.String("error", err.Error()),
		)

		// Close the channel to avoid errors when reconnecting
		wk.close()

		return amqppool.ErrNeedReconnect
	}
	return nil
}
