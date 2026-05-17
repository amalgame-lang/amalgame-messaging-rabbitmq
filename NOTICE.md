# NOTICE — amalgame-messaging-rabbitmq

## Authorship

Copyright 2026 Bastien Mouget. Original work — see
`runtime/Amalgame_Messaging_RabbitMQ.h`.

Part of the Amalgame ecosystem
([github.com/amalgame-lang/Amalgame](https://github.com/amalgame-lang/Amalgame)).
External contributions are paused at the ecosystem level; see the
main repo's `CONTRIBUTING.md` for the policy.

AI tools (Anthropic Claude) were used during development. Per
the project's authorship policy, AI is treated as a tool, not a
co-author at law.

## Licence

Apache License 2.0. See `LICENSE` for the full text.

## Third-party content

**None vendored.** This package binds to **librabbitmq** (also
known as **rabbitmq-c**), the official AMQP 0.9.1 C client
distributed by the RabbitMQ team. librabbitmq is provided by the
user's OS at compile time (via `librabbitmq-dev` /
`librabbitmq-devel` / `rabbitmq-c`) and at run time (via the
matching runtime package).

### librabbitmq / rabbitmq-c

Distributed under the [MIT License](https://github.com/alanxz/rabbitmq-c/blob/master/LICENSE-MIT)
— a permissive licence directly compatible with Apache-2.0
redistribution.

Upstream: [github.com/alanxz/rabbitmq-c](https://github.com/alanxz/rabbitmq-c).

This package does not include or redistribute any librabbitmq
code; users obtain it independently from their OS package manager
or from upstream.

## Trademarks

"RabbitMQ" is a trademark of VMware Inc. "Apache Qpid" and the
"AMQP" name are trademarks of the Apache Software Foundation.

This repository uses these names solely to identify the protocol
and broker implementations the package binds to. No trademark
claim is asserted.
