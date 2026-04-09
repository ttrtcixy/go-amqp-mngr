[English](README.md) · [Русский](README.ru.md)

A set of Go packages for working with RabbitMQ. Implements a connection pool with automatic reconnection, an async message publisher, and a resilient consumer — all with graceful shutdown support and retry logic on both the broker and application sides.

---

## Architecture

Three independent packages share a single pool implementation:

- **`amqppool`** — manages a single AMQP connection for all workers. Reconnects on failure; concurrent reconnect attempts are deduplicated via `singleflight`.
- **`sender`** — accepts tasks via `AsyncSend`, distributes them to workers through an in-memory channel, publishes to RabbitMQ in **confirm mode**, and on failure places the task into a **backlog** for retry.
- **`reader`** — multiple workers on a single pool, each subscribed to a queue via `Consume`. On processing error the message is sent to a **DLX → retry queue** and returned to the main queue after `TTL` expires.

---

## Broker Topology

Both packages use the same topology scheme: the main **direct** exchange accepts publications and routes them to the main queue. The main queue has `x-dead-letter-exchange` set to the retry exchange — messages go there after a `Nack`. The retry queue holds the message for `QueueRetryTTL`, then returns it to the main queue via DLX using the same routing key.

Topology is declared by the `SetupTopology` function from the `amqppool` package. With `SetupChannelEachUpdate: true` it is re-declared on every reconnect. With `false` — the objects must already exist on the broker with the same arguments.

> ⚠️ Queue arguments (e.g. DLX or TTL) cannot be changed without recreating the queue. When updating the config, either delete the queue manually or use a new name.

### `TopologyConfig`

| Field | Description |
|-------|-------------|
| `ExchangeName` | Main durable direct exchange. |
| `RetryExchangeName` | Exchange for delayed retries. |
| `QueueName` | Main queue; has a DLX pointing to the retry exchange. |
| `QueueRetryName` | Retry queue with `x-message-ttl` and a DLX back to the main exchange. |
| `QueueRetryTTL` | Message TTL in the retry queue. Default `1m`. |
| `RouteKey` | Routing key used for queue bindings and publishing. |

---

## Package amqppool

The core of the system. Manages the connection and worker lifecycle.

### Worker Interface

To use the pool your worker must implement the `Worker` interface:

```go
type Worker interface {
    // SetChannel is called by the pool when an AMQP channel is created or refreshed.
    // Store the channel here and configure Qos / confirm mode etc.
    SetChannel(ch *amqp.Channel) error

    // SetTopology is called once during pool initialization to verify
    // and declare the broker topology.
    SetTopology() error

    // Do is the main worker loop. Returns:
    //   - ErrNeedReconnect: channel/connection is invalid; the pool will refresh the channel and call Do again.
    //   - ErrClosed / ErrCtxClose: clean shutdown.
    //   - any other error: the worker stops.
    Do(ctx context.Context) error

    // IsInvalid returns true if the worker needs a new channel.
    IsInvalid() bool

    // Close releases worker resources (channel etc.).
    Close(ctx context.Context) error
}
```

### Errors

| Error | When to return from `Do` |
|-------|--------------------------|
| `ErrNeedReconnect` | Channel closed or confirmation timed out — a new channel is needed. |
| `ErrClosed` | Normal service shutdown. |
| `ErrCtxClose` | Application context cancelled. |

### Configuration

| Field | Description |
|-------|-------------|
| `URL` | RabbitMQ connection string. |
| `HeartBeatTimeout` | AMQP heartbeat interval on the connection. |
| `ConnectTimeout` | TCP dial timeout. |
| `ConnectKeepAlive` | TCP keep-alive interval. |
| `ReconnectInterval` | Pause after a failed reconnect before the next attempt. Avoid setting below one second in production. |
| `InsecureSkipVerify` | Skip TLS certificate verification. Dev only. |

### Starting a Worker

```go
pool, err := amqppool.New(ctx, &cfg.Pool, log, testWorker)
if err != nil {
    // failed to connect or declare topology
}

// Starts a goroutine with a reconnect loop. Blocks until the worker exits.
go pool.StartWorker(ctx, myWorker)

// Graceful shutdown — waits for all StartWorker goroutines to finish.
pool.Close(shutdownCtx)
```

---

## Package sender

Async message publishing to RabbitMQ with delivery guarantees via confirm mode and in-memory retry.

### How It Works

`AsyncSend` places a task into the main in-memory channel and blocks if it is full. If the context has no deadline, `DefaultSendTimeout` (default `100ms`) is applied automatically — this prevents indefinite blocking when the broker is unavailable and workers are not consuming.

Workers read from storage in priority order: **backlog first**, then the main channel — messages awaiting retry are processed first. Each worker operates in confirm mode: after publishing it waits for an ACK from the broker. If the confirmation does not arrive within `MessageConfirmTimeout`, the channel is closed and a reconnect is requested. This is intentional: a late ACK on a stale channel could confirm the wrong message. On any publish or confirmation error the task is placed in the backlog and the retry counter is decremented; once exhausted the task is dropped with a log entry.

### Quick Start

```go
cfg := sender.Config{
    Pool: amqppool.PoolConfig{
        URL:               "amqp://guest:guest@localhost:5672/",
        HeartBeatTimeout:  10 * time.Second,
        ConnectTimeout:    5 * time.Second,
        ConnectKeepAlive:  30 * time.Second,
        ReconnectInterval: 3 * time.Second,
    },
    Sender: sender.SenderConfig{
        WorkerCount:        3,
        DefaultSendTimeout: 100 * time.Millisecond,
        ShutdownTimeout:    10 * time.Second,
    },
    Worker: sender.WorkerConfig{
        TopologyConfig: amqppool.TopologyConfig{
            ExchangeName:      "emails",
            RetryExchangeName: "emails.retry",
            QueueName:         "emails.send",
            QueueRetryName:    "emails.send.retry",
            QueueRetryTTL:     time.Minute,
            RouteKey:          "send",
        },
        SetupChannelEachUpdate: true,
        MessageConfirmTimeout:  5 * time.Second,
        DefaultMessageTTL:      10 * time.Minute,
    },
    Storage: sender.StorageConfig{
        MainPoolMessageCount:       100,
        BacklogMessageCount:        50,
        BacklogMessageWriteTimeout: 500 * time.Millisecond,
        MaxMessageRetry:            3,
    },
}

s, err := sender.New(ctx, &cfg, log)
if err != nil {
    log.Fatal(err)
}
defer s.Close(shutdownCtx)
```

**Sending messages:**

```go
// Context without a deadline — DefaultSendTimeout (100ms) is applied
err := s.AsyncSend(context.Background(), &sender.Task{
    Body: jsonPayload,
})

// With an explicit enqueue timeout
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
err := s.AsyncSend(ctx, &sender.Task{
    TTL:  5 * time.Minute, // message TTL in RabbitMQ; if 0 — DefaultMessageTTL is used
    Body: jsonPayload,
})
```

### Configuration

#### `SenderConfig`

| Field | Description |
|-------|-------------|
| `WorkerCount` | Number of goroutines publishing to RabbitMQ. |
| `DefaultSendTimeout` | `AsyncSend` timeout when the context has no deadline. Default `100ms`. |
| `ShutdownTimeout` | Additional time limit during `Close` on top of the caller's context. `0` — context only. |

#### `WorkerConfig`

| Field | Description |
|-------|-------------|
| `SetupChannelEachUpdate` | Declare topology on every new channel. Disable if topology is managed externally. |
| `MessageConfirmTimeout` | Timeout waiting for an ACK from the broker. The channel is closed on expiry. |
| `DefaultMessageTTL` | Message TTL in RabbitMQ when `Task.TTL` is not set. Default `1m`. |

#### `StorageConfig`

| Field | Description |
|-------|-------------|
| `MainPoolMessageCount` | Size of the main channel between `AsyncSend` and workers. |
| `BacklogMessageCount` | Size of the channel for tasks awaiting retry. |
| `BacklogMessageWriteTimeout` | Timeout for writing to the backlog. `0` — wait indefinitely (not recommended). |
| `MaxMessageRetry` | Initial retry counter. At `0` the task is dropped immediately without any retry. |

---

## Package reader

Message consumer with multiple workers, DLX-retry, and graceful shutdown.

### How It Works

On startup a test worker is created to verify the connection and topology; then `WorkerCount` goroutines are launched, each with its own `MessageConsumer` instance (via the `ConsumerFactory`) — this allows every worker to have its own SMTP/HTTP client etc.

For each received message `consumer.Send` is called. On success — `Ack`. On error — `Nack(requeue=false)`, the message goes to the DLX and returns to the main queue after `QueueRetryTTL`. The retry count is tracked via the `x-death` header; once `MaxRetries` is exceeded the message is acknowledged (`Ack`) and dropped with an error log — infinite retry is not possible.

On `Close` the worker finishes processing the current message, cancels `Consume`, and exits. No new messages are taken from the queue.

### Quick Start

Implement the `MessageConsumer` interface:

```go
type MessageConsumer interface {
    Send(ctx context.Context, body []byte) error
    Close(ctx context.Context) error
}
```

```go
type EmailConsumer struct {
    smtp *smtp.Client
}

func (c *EmailConsumer) Send(ctx context.Context, body []byte) error {
    var email EmailPayload
    if err := json.Unmarshal(body, &email); err != nil {
        return err // returns Nack → retry
    }
    return c.smtp.Send(ctx, email)
}

func (c *EmailConsumer) Close(ctx context.Context) error {
    return c.smtp.Close()
}
```

Pass a factory to `reader.New` — each worker receives its own instance:

```go
cfg := reader.Config{
    Pool: amqppool.PoolConfig{ /* ... */ },
    Reader: reader.ReaderConfig{
        WorkerCount:     5,
        ShutdownTimeout: 15 * time.Second,
    },
    Worker: reader.WorkerConfig{
        TopologyConfig: amqppool.TopologyConfig{ /* ... */ },
        SetupChannelEachUpdate: true,
        MessageCount:           10,
        SendMessageTimeout:     30 * time.Second,
        MaxRetries:             5,
    },
}

r, err := reader.New(ctx, &cfg, log, func() reader.MessageConsumer {
    return &EmailConsumer{smtp: newSMTPClient()}
})
if err != nil {
    log.Fatal(err)
}
defer r.Close(shutdownCtx)
```

### Configuration

#### `ReaderConfig`

| Field | Description |
|-------|-------------|
| `WorkerCount` | Number of consumer goroutines. |
| `ShutdownTimeout` | Additional time limit during `Close` on top of the caller's context. |

#### `WorkerConfig`

| Field | Description |
|-------|-------------|
| `SetupChannelEachUpdate` | Declare topology on every new channel. |
| `MessageCount` | Prefetch count — how many messages a worker fetches from the queue at a time. |
| `SendMessageTimeout` | Timeout for processing a single message. `Send` receives a cancelled context on expiry. |
| `MaxRetries` | Maximum retry count based on the `x-death` header. On exceeded — Ack and drop. |
