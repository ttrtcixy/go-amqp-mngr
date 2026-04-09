package amqppool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/singleflight"
)

var (
	ErrCtxClose      = errors.New("context closed")
	ErrClosed        = errors.New("closed")
	ErrNeedReconnect = errors.New("need to reconnect")
)

type Worker interface {
	SetChannel(ch *amqp.Channel) error
	SetTopology() error

	// Do - any error other than ErrNeedReconnect terminates the worker.
	Do(ctx context.Context) error

	IsInvalid() bool
	Close(ctx context.Context) error
}

type PoolConfig struct {
	HeartBeatTimeout   time.Duration `env:"AMQP_HEARTBEAT_TIMEOUT,required"`
	ConnectTimeout     time.Duration `env:"AMQP_CONNECT_TIMEOUT,required"`
	ConnectKeepAlive   time.Duration `env:"AMQP_KEEP_ALIVE,required"`
	ReconnectInterval  time.Duration `env:"AMQP_RECONNECT_INTERVAL,required"`
	InsecureSkipVerify bool          `env:"AMQP_INSECURE_SKIP_VERIFY"        envDefault:"false"`
	URL                string        `env:"AMQP_URL,required"`
}

type Pool struct {
	cfg *PoolConfig

	log *slog.Logger

	mu   sync.RWMutex
	wg   sync.WaitGroup
	g    singleflight.Group
	conn *amqp.Connection

	stateCh chan *amqp.Error
}

func New(ctx context.Context, cfg *PoolConfig, log *slog.Logger, wk Worker) (p *Pool, err error) {
	const op = "amqppool.New"

	p = &Pool{
		cfg: cfg,
		log: log,
	}

	// crate conn to amqp server
	if err = p.connect(ctx); err != nil {
		return nil, fmt.Errorf("%s -> %w", op, err)
	}

	// test chan create and declare
	if err = p.refreshWorker(wk); err != nil {
		_ = p.conn.Close()

		return nil, fmt.Errorf("%s -> %w", op, err)
	}

	if err := wk.SetTopology(); err != nil {
		return nil, fmt.Errorf("%s - set topology error -> %w", op, err)
	}

	_ = wk.Close(ctx)

	return p, nil
}

func (p *Pool) connect(ctx context.Context) (err error) { // todo
	const op = "amqppool.connect"

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil && !p.conn.IsClosed() { // todo возможно тут ошибка
		return nil
	}

	if p.conn != nil {
		_ = p.conn.Close()
	}

	// only for dev
	tlsCfg := &tls.Config{
		InsecureSkipVerify: p.cfg.InsecureSkipVerify,
	}

	conn, err := amqp.DialConfig(p.cfg.URL, amqp.Config{
		Heartbeat:       p.cfg.HeartBeatTimeout,
		TLSClientConfig: tlsCfg,
		Properties: amqp.Table{
			"connection_name": "amqp.sender", // todo cfg
		},
		Dial: func(network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{
				Timeout:   p.cfg.ConnectTimeout,
				KeepAlive: p.cfg.ConnectKeepAlive,
			}
			return dialer.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		return fmt.Errorf("%s - error connect to amqp server -> %w", op, err)
	}

	p.conn = conn

	p.stateCh = conn.NotifyClose(make(chan *amqp.Error, 1))

	return nil
}

func (p *Pool) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}

	done := make(chan struct{}, 1)

	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		break
	case <-ctx.Done():
		break
	}

	if err := p.conn.Close(); err != nil {
		return err
	}

	return nil
}

func (p *Pool) isInvalid() bool { // todo
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.conn == nil || p.conn.IsClosed() {
		return true
	}

	select {
	case <-p.stateCh:
		return true
	default:
		return false
	}
}

func (p *Pool) StartWorker(ctx context.Context, wk Worker) {
	p.wg.Add(1)
	defer p.wg.Done()

	defer func() {
		if err := wk.Close(ctx); err != nil {
			p.log.LogAttrs(ctx, slog.LevelError, "error while closing worker", slog.String("error", err.Error()))
		}
	}()

	for {
		// check connection, reconnect if there is no connection.
		if err := p.ensureValidState(ctx, wk); err != nil { // todo other way to check connect state?
			if errors.Is(err, ErrCtxClose) {
				return
			}

			continue
		}

		if err := wk.Do(ctx); err != nil {
			if errors.Is(err, ErrNeedReconnect) {
				continue
			}

			if errors.Is(err, ErrClosed) || errors.Is(err, ErrCtxClose) {
				return
			}

			return
		}

		continue
	}
}

func (p *Pool) ensureValidState(ctx context.Context, wk Worker) (err error) { // todo
	const op = "amqppool.ensureValidState"

	defer func() {
		if err != nil {
			p.log.LogAttrs(
				ctx,
				slog.LevelError,
				"error ensuring valid state",
				slog.String("error", err.Error()),
			)

			select {
			case <-ctx.Done():
				err = ErrCtxClose

				return
			case <-time.After(p.cfg.ReconnectInterval):
				// block the worker for a while so as not to spam requests if the server has been down for a long time
				return
			}
		}
	}()

	if p.isInvalid() {
		if _, err, _ := p.g.Do("reconnect", func() (any, error) {
			// todo надо ли добавить доп таймаут на подключение? чтоб не держать mutex?
			return nil, p.connect(ctx)
		}); err != nil {
			return fmt.Errorf("%s - reconnect error -> %w", op, err)
		}
	}

	if wk.IsInvalid() {
		if err = p.refreshWorker(wk); err != nil {
			return fmt.Errorf("%s - refresh worker resources error -> %w", op, err)
		}
	}

	return nil
}

func (p *Pool) refreshWorker(wk Worker) error {
	const op = "amqppool.refreshWorker"

	p.mu.RLock()
	ch, err := p.conn.Channel()
	p.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("%s - create channel error -> %w", op, err)
	}

	if err := wk.SetChannel(ch); err != nil {
		return fmt.Errorf("%s - set channel error -> %w", op, err)
	}

	return nil
}
