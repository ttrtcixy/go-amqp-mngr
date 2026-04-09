package sender

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type StorageConfig struct {
	MainPoolMessageCount       int           `env:"SENDER_POOL_MESSAGE_COUNT,required"`
	BacklogMessageCount        int           `env:"SENDER_BACKLOG_MESSAGE_COUNT,required"`
	BacklogMessageWriteTimeout time.Duration `env:"SENDER_BACKLOG_MESSAGE_WRITE_TIMEOUT"`
	MaxMessageRetry            int8          `env:"SENDER_MAX_MESSAGE_SEND_RETRY,required"`
}

type msg struct {
	ctx      context.Context // using for request_id or trace_id. If it in value
	task     *Task
	attempts int8
}

type storage struct {
	cfg *StorageConfig
	log *slog.Logger

	mu     sync.RWMutex
	closed bool

	msgs    chan msg
	backlog chan msg
}

func newStorage(cfg *StorageConfig, log *slog.Logger) *storage {
	return &storage{
		cfg:     cfg,
		log:     log,
		msgs:    make(chan msg, cfg.MainPoolMessageCount),
		backlog: make(chan msg, cfg.BacklogMessageCount),
	}
}

func (s *storage) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	s.closed = true

	s.mu.Unlock()
	close(s.msgs)
	close(s.backlog)
}

func (s *storage) push(ctx context.Context, m msg) error {
	const op = "sender.push"

	s.mu.RLock()
	defer s.mu.RUnlock()

	select {
	case s.msgs <- m:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s - push message error, ctx done -> %w", op, ctx.Err())
	}
}

// pushInBacklog - put message in backlog
func (s *storage) pushInBacklog(ctx context.Context, m msg) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		s.log.LogAttrs(
			m.ctx,
			slog.LevelError,
			"push msg to backlog error, Close signal",
		)
		return
	}

	if m.attempts == 0 {
		s.log.LogAttrs(
			m.ctx,
			slog.LevelWarn,
			"push msg to backlog error, rewrite attempts ended",
		)
		return
	}

	m.attempts--

	if s.cfg.BacklogMessageWriteTimeout != 0 {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, s.cfg.BacklogMessageWriteTimeout)

		defer cancel()
	}

	select {
	case s.backlog <- m:
		return
	case <-ctx.Done():
		s.log.LogAttrs(
			m.ctx,
			slog.LevelError,
			"backlog is full, message lost",
			slog.String("error", ctx.Err().Error()),
		)
		return
	}
}

// GetMsg - receiving from the channel, priority is given to data from the backlog
func (s *storage) getMsg(ctx context.Context) (msg, bool) {
	if err := ctx.Err(); err != nil {
		return msg{}, false
	}

	select { // check the message queue for resending
	case m, ok := <-s.backlog:
		if ok {
			return m, true
		}
	default:
	}

	select {
	case m, ok := <-s.backlog: // check the queue for re-sending.
		if ok {
			return m, true
		}
		// if there is a message about closing, it means the channel has been read, you need to check whether the message channel has been read.
		m, ok = <-s.msgs
		return m, ok

	case m, ok := <-s.msgs: // check the message queue.
		if ok {
			return m, true
		}
		// if there is a message about closing, it means the channel has been read, you need to check whether the channel has been read before resending.
		m, ok = <-s.backlog
		return m, ok

	case <-ctx.Done():
		return msg{}, false
	}
}

func (s *storage) message(ctx context.Context, t *Task) msg {
	return msg{
		ctx:      ctx,
		task:     t,
		attempts: s.cfg.MaxMessageRetry,
	}
}
