# amalgame-messaging-rabbitmq

RabbitMQ / AMQP 0.9.1 client for [Amalgame](https://github.com/amalgame-lang/Amalgame).
Dynamic-linked to **librabbitmq** (rabbitmq-c, the official C
client distributed by the RabbitMQ team). Works against any
AMQP 0.9.1 broker: **RabbitMQ, Apache Qpid, CloudAMQP**, etc.

Fifth amalgame-lang dynamic-link package (after postgresql /
mysql / mongodb / kafka), fourth in the messaging tier alongside
MQTT, NATS, Kafka.

## AMQP 0.9.1 has 4 core concepts (vs MQTT/NATS's 2)

- **Exchange** — where you publish to (with a type: `direct`,
  `fanout`, `topic`, `headers`)
- **Queue** — where consumers read from
- **Binding** — link between an Exchange and a Queue with a
  routing-key filter
- **Message** — payload + routing-key

That extra ceremony unlocks routing patterns MQTT/NATS don't
express natively (pub/sub via fanout, per-key worker queues via
direct, wildcard topic trees via topic). v1 exposes the
primitives; users compose patterns at the call site.

## Prerequisites

| OS / distro | Command |
|---|---|
| Debian / Ubuntu | `sudo apt install librabbitmq-dev` |
| Fedora / RHEL / Rocky | `sudo dnf install librabbitmq-devel` |
| Arch / Manjaro | `sudo pacman -S rabbitmq-c` |
| Alpine | `apk add rabbitmq-c-dev` |
| macOS (Homebrew) | `brew install rabbitmq-c` |
| Windows (MSYS2) | `pacman -S mingw-w64-x86_64-rabbitmq-c` |

Header layout varies: Debian ships `<amqp.h>` flat,
Fedora / Arch / brew use the `<rabbitmq-c/amqp.h>` sub-dir
convention since rabbitmq-c 0.13. The wrapper uses
`__has_include` so both work without `-I` tweaking.

On the **deploy** machine you need the runtime variant
(`librabbitmq4` on Debian etc.). Typically pulled in as a dep of
the dev package.

## Install

```bash
amc package add rabbitmq                                       # via index
amc package add github.com/amalgame-lang/amalgame-messaging-rabbitmq@v0.1.0
```

Requires **amc 0.8.19+**.

## Surface

```amalgame
import Amalgame.Messaging.RabbitMQ

let r = RabbitMQ.Connect("127.0.0.1", 5672, "guest", "guest", "/")
if (!RabbitMQ.IsOpen(r)) {
    Console.WriteLine("connect failed: " + RabbitMQ.LastError(r))
    return
}

// Topology
RabbitMQ.DeclareExchange(r, "events", "topic")
let q = RabbitMQ.DeclareQueue(r, "")            // server auto-names
RabbitMQ.Bind(r, q, "events", "user.*")          // wildcard routing-key

// Publish
RabbitMQ.Publish(r, "events", "user.login",  "{\"id\":42}")
RabbitMQ.Publish(r, "events", "user.logout", "{\"id\":42}")

// Consume
RabbitMQ.Consume(r, q)
while (RabbitMQ.Poll(r, 2000)) {
    Console.WriteLine(RabbitMQ.LastRoutingKey(r)
        + " = " + RabbitMQ.LastBody(r))
    RabbitMQ.Ack(r)
}

RabbitMQ.Close(r)
```

### v0.1.0 method surface

| Method | Returns | Notes |
|---|---|---|
| `RabbitMQ.Connect(host, port, user, password, vhost)` | `AmalgameRabbitMQ*` | PLAIN auth; `vhost=""` defaults to `"/"` |
| `RabbitMQ.Close(r)` | `void` | Closes channel #1 + connection |
| `RabbitMQ.IsOpen(r)` | `bool` | |
| `RabbitMQ.LastError(r)` | `string` | Empty on success |
| `RabbitMQ.DeclareExchange(r, name, type)` | `bool` | type ∈ `direct` / `fanout` / `topic` / `headers` |
| `RabbitMQ.DeclareQueue(r, name)` | `string` | Returns final queue name; `""` → server auto-names |
| `RabbitMQ.Bind(r, queue, exchange, routingKey)` | `bool` | |
| `RabbitMQ.Publish(r, exchange, routingKey, body)` | `bool` | |
| `RabbitMQ.Consume(r, queue)` | `bool` | Start the consumer |
| `RabbitMQ.Poll(r, timeout_ms)` | `bool` | True if message received |
| `RabbitMQ.LastBody / LastRoutingKey / LastExchange` | `string` | Last polled fields |
| `RabbitMQ.LastDeliveryTag(r)` | `int` | For Ack |
| `RabbitMQ.Ack(r)` | `bool` | Ack the last polled message |

### Exchange types — quick reference

| Type | Routing behaviour |
|---|---|
| `direct` | exact match on routing key → queue's binding key |
| `fanout` | broadcast to every bound queue, routing key ignored |
| `topic` | wildcard match: `*` = one word, `#` = zero or more words (`user.*`, `*.errors.#`) |
| `headers` | match on message headers (not routing key) — rarely used |

## Default settings

- **Channel** — single channel (#1) per connection. Multi-channel
  surface lands in v2.
- **Queue declared** with `auto_delete=true` (cleaned up after
  the consumer disconnects), `durable=false` (vanishes on broker
  restart). v2 exposes the flags so durable + persistent
  topologies work.
- **Heartbeat** — disabled (0). librabbitmq's blocking I/O
  doesn't run a background heartbeat thread; v2 exposes the
  tuning knob.
- **Messages** — `delivery_mode=1` (transient). v2 surfaces
  persistent (`delivery_mode=2`) when durable queues land.

## Deferred to v2

- Multiple channels per connection (we use channel #1)
- Publisher confirms (`confirm.select` + `basic.ack` callbacks)
- Transactions (`tx.select` / `commit` / `rollback`)
- `basic.properties` (priority, expiration, message-id, headers)
- Durable queues + persistent messages
- Dead-letter exchanges + queue arguments
- TLS (amqps://)
- SASL methods beyond PLAIN (EXTERNAL, AMQPLAIN, …)
- Connection blocking notifications

## Threading

`AmalgameRabbitMQ*` is single-owner. librabbitmq is **not**
thread-safe per-connection; use distinct handles across threads.

## Tests

```bash
./tests/run_tests.sh /path/to/amc
```

Double-gated: `librabbitmq-dev` + reachable broker at
`127.0.0.1:5672` (or `$RABBITMQ_HOST:$RABBITMQ_PORT`). Start
locally:

```bash
docker run --rm -d -p 5672:5672 rabbitmq:4
export RABBITMQ_HOST=127.0.0.1
```

13 cases exercise the full Connect → Declare → Bind → Publish →
Consume → Poll → Ack → Close round-trip.

## Licence

Apache-2.0 — see [`LICENSE`](LICENSE) and [`NOTICE.md`](NOTICE.md).
librabbitmq itself is MIT — directly compatible.
