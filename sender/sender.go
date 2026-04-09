package sender

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqppool "github.com/ttrtcixy/go-amqp-mngr"
)

type Config struct {
	Pool    amqppool.PoolConfig
	Sender  SenderConfig
	Worker  WorkerConfig
	Storage StorageConfig
}

type SenderConfig struct {
	DefaultSendTimeout time.Duration `env:"SENDER_DEFAULT_SEND_TIMEOUT"      envDefault:"100ms"`
	WorkerCount        int           `env:"SENDER_WORKER_COUNT,required"`
	ShutdownTimeout    time.Duration `env:"SENDER_SHUTDOWN_TIMEOUT,required"`
}

type Sender struct {
	pool *amqppool.Pool

	log *slog.Logger
	cfg *Config

	mu sync.RWMutex

	closed bool

	storage *storage
}

func New(ctx context.Context, cfg *Config, log *slog.Logger) (*Sender, error) {
	const op = "sender.New"

	st := newStorage(&cfg.Storage, log)

	wk := newWorker(&cfg.Worker, log, st)

	p, err := amqppool.New(ctx, &cfg.Pool, log, wk)
	if err != nil {
		st.close()
		return nil, fmt.Errorf("%s -> %w", op, err)
	}

	s := &Sender{
		pool:    p,
		cfg:     cfg,
		log:     log,
		storage: st,
	}

	s.start(ctx)

	return s, nil
}

type Task struct {
	TTL  time.Duration
	Body []byte
}

// todo fix double log  message

func (s *Sender) AsyncSend(ctx context.Context, t *Task) error {
	const op = "sender.AsyncSend"

	if t == nil {
		return fmt.Errorf("%s - invalid task", op)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errors.New("amqp service closed")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, s.cfg.Sender.DefaultSendTimeout)

		defer cancel()
	}

	if err := s.storage.push(ctx, s.storage.message(ctx, t)); err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	return nil
}

func (s *Sender) start(ctx context.Context) {
	for range s.cfg.Sender.WorkerCount {
		go func() {
			wk := newWorker(&s.cfg.Worker, s.log, s.storage)

			s.pool.StartWorker(ctx, wk)
		}()
	}
}

func (s *Sender) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}

	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()
		return nil
	}

	s.closed = true

	s.storage.close()

	s.mu.Unlock()

	if s.cfg.Sender.ShutdownTimeout != 0 {
		if t, ok := ctx.Deadline(); ok {
			if t.Sub(time.Now()) < s.cfg.Sender.ShutdownTimeout {
				s.log.LogAttrs(
					ctx,
					slog.LevelWarn,
					"The passed context expires before the s.cfg.Sender.ShutdownTimeout parameter",
					slog.String("op", "sender.Close"),
				)
			}
		}

		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, s.cfg.Sender.ShutdownTimeout)

		defer cancel()
	}

	return s.pool.Close(ctx)
}
