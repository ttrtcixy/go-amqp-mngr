[English](README.md) · [Русский](README.ru.md)

Набор Go-пакетов для работы с RabbitMQ. Реализует пул соединений с автоматическим переподключением, асинхронный публикатор сообщений и устойчивого потребителя — всё с поддержкой graceful shutdown и retry-логикой на стороне как брокера, так и приложения.

---

## Архитектура

Три независимых пакета используют одну кодовую базу пула:

- **`amqppool`** — управляет одним AMQP-соединением для всех воркеров. При разрыве переподключается; одновременные попытки переподключения дедуплицируются через `singleflight`.
- **`sender`** — принимает задачи через `AsyncSend`, раскладывает их по воркерам через in-memory канал, публикует в RabbitMQ в **confirm mode** и при неудаче помещает задачу в **backlog** для повторной попытки.
- **`reader`** — несколько воркеров на одном пуле, каждый подписан на очередь через `Consume`. При ошибке обработки сообщение уходит в **DLX → retry queue**, откуда возвращается в основную очередь после `TTL`.

---

## Топология брокера

Оба пакета используют одну схему: основной **direct** exchange принимает публикации и направляет их в основную очередь. У основной очереди настроен `x-dead-letter-exchange` на retry exchange — туда уходят сообщения после `Nack`. Retry queue держит сообщение `QueueRetryTTL`, после чего через DLX возвращает его обратно в основную очередь по тому же routing key.

Топология объявляется функцией `SetupTopology` из пакета `amqppool`. При `SetupChannelEachUpdate: true` она объявляется заново при каждом переподключении. При `false` — объекты должны уже существовать на брокере с теми же аргументами.

> ⚠️ Изменить аргументы существующей очереди (например, DLX или TTL) без её пересоздания нельзя. При изменении конфига либо удаляйте очередь вручную, либо используйте новое имя.

### `TopologyConfig`

| Поле | Описание |
|------|----------|
| `ExchangeName` | Основной durable direct exchange. |
| `RetryExchangeName` | Exchange для отложенных повторов. |
| `QueueName` | Основная очередь; к ней привязан DLX на retry exchange. |
| `QueueRetryName` | Retry-очередь с `x-message-ttl` и DLX обратно на основной exchange. |
| `QueueRetryTTL` | TTL сообщения в retry-очереди. По умолчанию `1m`. |
| `RouteKey` | Ключ маршрутизации для привязок очередей и публикации. |

---

## Пакет amqppool

Ядро системы. Управляет соединением и жизненным циклом воркеров.

### Интерфейс Worker

Для использования пула ваш воркер должен реализовать интерфейс `Worker`:

```go
type Worker interface {
    // SetChannel вызывается пулом при создании или обновлении AMQP-канала.
    // Здесь нужно сохранить канал, настроить Qos / confirm mode и т.д.
    SetChannel(ch *amqp.Channel) error

    // SetTopology вызывается один раз при инициализации пула для проверки
    // и объявления топологии на брокере.
    SetTopology() error

    // Do — основной цикл работы воркера. Возвращает:
    //   - ErrNeedReconnect: канал/соединение инвалидно, пул обновит канал и вызовет Do снова.
    //   - ErrClosed / ErrCtxClose: штатное завершение.
    //   - любая другая ошибка: воркер останавливается.
    Do(ctx context.Context) error

    // IsInvalid возвращает true, если воркеру нужен новый канал.
    IsInvalid() bool

    // Close освобождает ресурсы воркера (канал и т.д.).
    Close(ctx context.Context) error
}
```

### Ошибки

| Ошибка | Когда возвращать из `Do` |
|--------|--------------------------|
| `ErrNeedReconnect` | Канал закрылся, подтверждение не пришло вовремя — нужен новый канал. |
| `ErrClosed` | Штатная остановка сервиса. |
| `ErrCtxClose` | Контекст приложения отменён. |

### Конфигурация

| Поле | Описание |
|------|----------|
| `URL` | Строка подключения к RabbitMQ. |
| `HeartBeatTimeout` | Heartbeat AMQP на соединении. |
| `ConnectTimeout` | Таймаут установки TCP-соединения. |
| `ConnectKeepAlive` | Интервал keep-alive для TCP. |
| `ReconnectInterval` | Пауза после ошибки переподключения перед следующей попыткой. Не стоит ставить меньше секунды в production. |
| `InsecureSkipVerify` | Пропуск проверки TLS-сертификата. Только для dev. |

### Запуск воркера

```go
pool, err := amqppool.New(ctx, &cfg.Pool, log, testWorker)
if err != nil {
    // не удалось подключиться или объявить топологию
}

// Запускает горутину с циклом переподключения. Блокируется до выхода воркера.
go pool.StartWorker(ctx, myWorker)

// Graceful shutdown — ждёт завершения всех StartWorker-горутин.
pool.Close(shutdownCtx)
```

---

## Пакет sender

Асинхронная публикация сообщений в RabbitMQ с гарантией доставки через confirm mode и in-memory retry.

### Как работает

`AsyncSend` кладёт задачу в основной in-memory канал и блокируется, если он полон. Если у переданного контекста нет дедлайна, автоматически применяется `DefaultSendTimeout` (по умолчанию `100ms`) — защита от бесконечного ожидания, когда брокер недоступен и воркеры не читают очередь.

Воркеры читают из storage в приоритете: сначала **backlog**, затем основной канал — сообщения, ожидающие повтора, обрабатываются первыми. Каждый воркер работает в confirm mode: после публикации ждёт ACK от брокера. Если подтверждение не пришло за `MessageConfirmTimeout`, канал закрывается и запрашивается переподключение — это намеренное поведение, так как "запоздалый" ACK на старом канале мог бы подтвердить не то сообщение. При любой ошибке публикации или подтверждения задача уходит в backlog, счётчик попыток уменьшается; при исчерпании задача отбрасывается с логом.

### Быстрый старт

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

**Отправка:**

```go
// Контекст без дедлайна — применяется DefaultSendTimeout (100ms)
err := s.AsyncSend(context.Background(), &sender.Task{
    Body: jsonPayload,
})

// С явным таймаутом на постановку в очередь
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()
err := s.AsyncSend(ctx, &sender.Task{
    TTL:  5 * time.Minute, // время жизни в RabbitMQ; если 0 — используется DefaultMessageTTL
    Body: jsonPayload,
})
```

### Конфигурация

#### `SenderConfig`

| Поле | Описание |
|------|----------|
| `WorkerCount` | Количество горутин, публикующих в RabbitMQ. |
| `DefaultSendTimeout` | Таймаут `AsyncSend`, когда у контекста нет дедлайна. По умолчанию `100ms`. |
| `ShutdownTimeout` | Дополнительный лимит времени при `Close` поверх переданного контекста. `0` — только контекст. |

#### `WorkerConfig`

| Поле | Описание |
|------|----------|
| `SetupChannelEachUpdate` | Объявлять топологию при каждом новом канале. Отключить, если топология управляется отдельно. |
| `MessageConfirmTimeout` | Таймаут ожидания ACK от брокера. По истечении канал закрывается. |
| `DefaultMessageTTL` | TTL сообщения в RabbitMQ, если в `Task.TTL` не задано. По умолчанию `1m`. |

#### `StorageConfig`

| Поле | Описание |
|------|----------|
| `MainPoolMessageCount` | Размер основного канала между `AsyncSend` и воркерами. |
| `BacklogMessageCount` | Размер канала для задач, ожидающих повторной отправки. |
| `BacklogMessageWriteTimeout` | Таймаут записи в backlog. `0` — ждать бесконечно (не рекомендуется). |
| `MaxMessageRetry` | Начальное число попыток. При `0` задача сразу отбрасывается без повтора. |

---

## Пакет reader

Потребитель сообщений с несколькими воркерами, DLX-retry и graceful shutdown.

### Как работает

При старте создаётся тестовый воркер для проверки соединения и топологии; затем запускаются `WorkerCount` горутин, каждая со своим экземпляром `MessageConsumer` (через фабрику `ConsumerFactory`) — это позволяет каждому воркеру иметь собственный SMTP/HTTP-клиент и т.д.

Для каждого полученного сообщения вызывается `consumer.Send`. При успехе — `Ack`. При ошибке — `Nack(requeue=false)`, сообщение уходит в DLX и через `QueueRetryTTL` возвращается в основную очередь. Количество попыток отслеживается через заголовок `x-death`; при превышении `MaxRetries` сообщение подтверждается (`Ack`) и удаляется с логом ошибки — бесконечный retry невозможен.

При вызове `Close` воркер завершает обработку текущего сообщения, отменяет `Consume` и выходит. Новые сообщения из очереди не берутся.

### Быстрый старт

Реализуйте интерфейс `MessageConsumer`:

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
        return err // вернёт Nack → retry
    }
    return c.smtp.Send(ctx, email)
}

func (c *EmailConsumer) Close(ctx context.Context) error {
    return c.smtp.Close()
}
```

Передайте фабрику в `reader.New` — каждый воркер получит свой экземпляр:

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

### Конфигурация

#### `ReaderConfig`

| Поле | Описание |
|------|----------|
| `WorkerCount` | Количество горутин-потребителей. |
| `ShutdownTimeout` | Дополнительный лимит при `Close` поверх переданного контекста. |

#### `WorkerConfig`

| Поле | Описание |
|------|----------|
| `SetupChannelEachUpdate` | Объявлять топологию при каждом новом канале. |
| `MessageCount` | Prefetch count — сколько сообщений воркер берёт из очереди за раз. |
| `SendMessageTimeout` | Таймаут на обработку одного сообщения. По истечении `Send` получит отменённый контекст. |
| `MaxRetries` | Максимальное число повторов по заголовку `x-death`. При превышении — Ack и удаление. |
