# Async Email Sender via RabbitMQ

[English](README.md) · [Русский](README.ru.md)

Package for asynchronous email sending via RabbitMQ. Accepts `EmailTask`, queues it, workers publish to broker with delivery confirmation. Actual SMTP delivery is handled by queue consumers.

---

## How it works

1. **`AsyncSend`** puts task into internal queue (blocks if full)
   - If context has no timeout, uses `DefaultSendTimeout` (100ms default)
   - This protects from infinite waiting when queue is overloaded
   
2. **Workers** take tasks, publish to RabbitMQ with confirmation
   - All workers share one server connection
   - Each worker writes to its own AMQP channel
   - Uses **confirm mode** — waits for delivery confirmation from broker
   - Auto-reconnects with `ReconnectInterval` pause on connection loss
   
3. On errors task goes to **backlog** for retry
   - Publishing errors, missing confirmation or timeouts
   - Each retry attempt decreases `MaxMessageRetry` counter
   
4. **Retry attempts** are processed with priority over new tasks
   - Workers check backlog first, then main queue
   - This ensures problematic messages are handled first
   
5. **`Close`** gracefully stops all workers and closes connections
   - Stops accepting new tasks, waits for current operations to finish
   - Uses `ShutdownTimeout` to limit shutdown time

---

## Configuration

### `SenderConfig` (`Config.Sender`)

| Field | What it controls |
|-------|-------------------|
| **`WorkerCount`** | Number of worker goroutines that take tasks and publish to RabbitMQ. |
| **`ShutdownTimeout`** | Time limit for **`Close`** call (in addition to passed context). Zero means no extra deadline besides caller context. |
| **`DefaultSendTimeout`** | Default timeout for send operation. |

### `WriterConfig` (`Config.Writer`)

| Field | What it controls |
|-------|-------------------|
| **`URL`** | RabbitMQ connection string. |
| **`QueueName`** | Queue name when declaring topology (durable queue). |
| **`ExchangeName`** | Direct exchange name when declaring topology (durable). |
| **`RouteKey`** | Routing key for binding queue to exchange and publishing messages. |
| **`SetupChannelEachUpdate`** | Whether to declare queue, exchange, and binding on each new channel. If disabled, topology must already exist on broker. |
| **`ConnectTimeout`** | TCP connection timeout to broker. |
| **`ConnectKeepAlive`** | Keep-alive interval for TCP connection. |
| **`HeartBeatTimeout`** | AMQP heartbeat interval on connection. |
| **`ReconnectInterval`** | Pause after failed reconnect/channel refresh before next attempt. |
| **`MessageConfirmTimeout`** | How long to wait for publish confirmation after sending; on timeout publish is treated as failed and task may be retried via backlog. |
| **`DefaultMessageTTL`** | Default message TTL in RabbitMQ. |

### `StorageConfig` (`Config.Storage`)

| Field | What it controls |
|-------|-------------------|
| **`MainPoolMessageCount`** | Size of main in-memory queue between **`AsyncSend`** and workers. |
| **`BacklogMessageCount`** | Size of backlog queue for failed tasks awaiting retry. |
| **`BacklogMessageWriteTimeout`** | Optional time limit when writing failed task to backlog (to avoid blocking if backlog stays full). Zero means no additional limit. |
| **`MaxMessageRetry`** | Initial retry counter for tasks; decreases on each failure and requeue to backlog. At zero task is not requeued again. |

Environment variable names for loading fields are specified in `env` tags in `sender.go`, `writer.go`, and `messages.go`.

---

## API

```go
// Basic usage
sender.AsyncSend(ctx, &EmailTask{
    TTL:  5 * time.Minute,  // optional, otherwise DefaultMessageTTL
    Body: jsonEmailData,     // JSON for consumer
})

// Without context or timeout - uses DefaultSendTimeout
sender.AsyncSend(context.Background(), task)  // 100ms timeout
sender.AsyncSend(context.TODO(), task)        // 100ms timeout

// With explicit timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
sender.AsyncSend(ctx, task)  // 5s timeout
```

### Methods

- **`New(ctx, cfg, log)`** — creates sender, connects to RabbitMQ, starts workers
  - Establishes connection and tests channel creation
  - Starts `WorkerCount` goroutines for task processing
  
- **`AsyncSend(ctx, task)`** — queues task
  - Blocks if main queue is full
  - Automatically adds `DefaultSendTimeout` if context has no deadline
  - Returns error if service is closed or context is cancelled
  
- **`Close(ctx)`** — stops accepting tasks, waits for workers to finish
  - Closes internal queues, workers finish current operations
  - Uses `ShutdownTimeout` as additional time limit
