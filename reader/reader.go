package reader

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqppool "github.com/ttrtcixy/go-amqp-mngr"
)

type Config struct {
	Pool   amqppool.PoolConfig
	Reader ReaderConfig
	Worker WorkerConfig
}

type ReaderConfig struct {
	WorkerCount     int           `env:"EMAIL_READER_WORKER_COUNT,required"`
	ShutdownTimeout time.Duration `env:"EMAIL_READER_SHUTDOWN_TIMEOUT,required"`
}

type Reader struct {
	pool *amqppool.Pool

	// for soft shutdown, if channel closed, the worker must begin shutting down after reading the messages
	stopCh chan struct{}

	log *slog.Logger
	cfg *Config

	mu     sync.RWMutex
	closed bool
}

type ConsumerFactory func() MessageConsumer

func New(ctx context.Context, cfg *Config, log *slog.Logger, consumer ConsumerFactory) (*Reader, error) {
	const op = "reader.New"

	stopCh := make(chan struct{})

	// test worker
	wk := newWorker(&cfg.Worker, log, consumer(), stopCh)

	p, err := amqppool.New(ctx, &cfg.Pool, log, wk)
	if err != nil {
		return nil, fmt.Errorf("%s -> %w", op, err)
	}

	r := &Reader{
		pool:   p,
		cfg:    cfg,
		log:    log,
		stopCh: stopCh,
	}

	r.start(ctx, consumer, stopCh)

	return r, nil
}

func (r *Reader) start(ctx context.Context, consumer ConsumerFactory, stopCh chan struct{}) {
	for range r.cfg.Reader.WorkerCount {
		go func() {
			wk := newWorker(&r.cfg.Worker, r.log, consumer(), stopCh)

			r.pool.StartWorker(ctx, wk)
		}()
	}
}

func (r *Reader) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true

	close(r.stopCh)

	if r.cfg.Reader.ShutdownTimeout != 0 {
		if t, ok := ctx.Deadline(); ok {
			if time.Until(t) < r.cfg.Reader.ShutdownTimeout {
				r.log.LogAttrs(
					ctx,
					slog.LevelWarn,
					"The passed context expires before the r.cfg.Reader.ShutdownTimeout parameter",
					slog.String("op", "reader.Close"),
				)
			}
		}

		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, r.cfg.Reader.ShutdownTimeout)

		defer cancel()
	}

	return r.pool.Close(ctx)
}
