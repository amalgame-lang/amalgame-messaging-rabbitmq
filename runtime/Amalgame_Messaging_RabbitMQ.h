/*
 * Amalgame Standard Library — Amalgame.Messaging.RabbitMQ
 * Copyright (c) 2026 Bastien MOUGET
 * https://github.com/amalgame-lang/Amalgame
 *
 * RabbitMQ / AMQP 0.9.1 binding — dynamic-linked to librabbitmq
 * (also called rabbitmq-c, the official C client distributed by
 * the RabbitMQ team / Pivotal / VMware). Works against any AMQP
 * 0.9.1 broker (RabbitMQ, Apache Qpid, CloudAMQP, etc.).
 *
 * Surface (v1):
 *   Connect(host, port, user, password, vhost) / Close / IsOpen
 *   LastError
 *
 *   DeclareExchange(name, type)                   direct/fanout/topic/headers
 *   DeclareQueue(name)            → final name    "" → server auto-names
 *   Bind(queue, exchange, routingKey)
 *
 *   Publish(exchange, routingKey, body)
 *   Consume(queue)                                start the consumer
 *   Poll(timeout_ms)                              blocking-receive
 *   LastBody / LastRoutingKey / LastExchange / LastDeliveryTag
 *   Ack                                           ack the last polled msg
 *
 * Threading: AmalgameRabbitMQ* is single-owner. librabbitmq is
 * not thread-safe per-connection; use distinct handles across
 * threads. v1 uses a single channel (#1) per connection — multi-
 * channel surface lands in v2.
 *
 * Memory: each handle owns the amqp_connection_state_t + the
 * latest envelope (Poll consumes the previous one before grabbing
 * the next). GC finalizer wraps amqp_connection_close +
 * amqp_destroy_connection so a leaked handle still releases
 * sockets eventually.
 *
 * Out of scope (v2): multi-channel per connection, publisher
 * confirms, transactions, basic.properties (headers / persistent /
 * priority), durable queues + persistent msgs (everything is
 * transient in v1), dead-letter exchanges, TLS (amqps://), SASL
 * mechanisms beyond PLAIN.
 */

#ifndef AMALGAME_MESSAGING_RABBITMQ_H
#define AMALGAME_MESSAGING_RABBITMQ_H

#include "_runtime.h"
#include "Amalgame_Collections.h"

/* Header layout differs across distros:
 *   Debian / Ubuntu: <amqp.h> + <amqp_tcp_socket.h> in /usr/include
 *   Fedora / Arch / brew: <rabbitmq-c/amqp.h> + <rabbitmq-c/tcp_socket.h>
 *     (modern convention since librabbitmq 0.13)
 * We try the modern layout first, fall back to the flat one. */
#if defined(__has_include)
#  if __has_include(<rabbitmq-c/amqp.h>)
#    include <rabbitmq-c/amqp.h>
#    include <rabbitmq-c/tcp_socket.h>
#    include <rabbitmq-c/framing.h>
#  elif __has_include(<amqp.h>)
#    include <amqp.h>
#    include <amqp_tcp_socket.h>
#    include <amqp_framing.h>
#  else
#    error "amqp.h not found. Install librabbitmq-dev / librabbitmq-devel / rabbitmq-c."
#  endif
#else
#  include <amqp.h>
#  include <amqp_tcp_socket.h>
#  include <amqp_framing.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define AMALGAME_RMQ_CHANNEL 1   /* single channel for v1 */

typedef struct AmalgameRabbitMQ {
    amqp_connection_state_t conn;       /* NULL once Close'd */
    amqp_socket_t*          sock;
    char*                   last_error;
    /* Last Poll'd message — kept around so Ack can use the
     * delivery_tag, and so the AM facade can read body/key
     * without immediate free. */
    char*                   last_body;
    char*                   last_rk;
    char*                   last_exchange;
    i64                     last_delivery_tag;
    int                     have_envelope;
    amqp_envelope_t         envelope;
} AmalgameRabbitMQ;

/* ── Helpers ────────────────────────────────────────── */

static inline code_string _amrmq_str_dup(const char* s) {
    if (!s) return NULL;
    size_t n = strlen(s);
    char* p = (char*) code_alloc(n + 1);
    memcpy(p, s, n + 1);
    return p;
}

static inline code_string _amrmq_bytes_dup(amqp_bytes_t b) {
    if (!b.bytes || b.len == 0) return _amrmq_str_dup("");
    char* p = (char*) code_alloc(b.len + 1);
    memcpy(p, b.bytes, b.len);
    p[b.len] = '\0';
    return p;
}

/* Pretty-print an amqp_rpc_reply_t / amqp_status_enum to a
 * GC-allocated string. For server exceptions, decode the method
 * id and pull reply_code + reply_text out of the decoded struct
 * — otherwise the user only sees an opaque "method 655410"
 * number which is unhelpful at the call site. */
static inline code_string _amrmq_reply_err(amqp_rpc_reply_t r) {
    if (r.reply_type == AMQP_RESPONSE_NORMAL) return _amrmq_str_dup("");
    if (r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
        const char* msg = amqp_error_string2(r.library_error);
        return _amrmq_str_dup(msg ? msg : "AMQP library error");
    }
    if (r.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION) {
        char buf[512];
        if (r.reply.id == AMQP_CONNECTION_CLOSE_METHOD) {
            amqp_connection_close_t* m = (amqp_connection_close_t*) r.reply.decoded;
            snprintf(buf, sizeof(buf),
                     "AMQP connection.close %d: %.*s",
                     m->reply_code,
                     (int)(m->reply_text.len > 400 ? 400 : (int) m->reply_text.len),
                     (const char*) m->reply_text.bytes);
        } else if (r.reply.id == AMQP_CHANNEL_CLOSE_METHOD) {
            amqp_channel_close_t* m = (amqp_channel_close_t*) r.reply.decoded;
            snprintf(buf, sizeof(buf),
                     "AMQP channel.close %d: %.*s",
                     m->reply_code,
                     (int)(m->reply_text.len > 400 ? 400 : (int) m->reply_text.len),
                     (const char*) m->reply_text.bytes);
        } else {
            snprintf(buf, sizeof(buf), "AMQP server exception (method 0x%X)",
                     (unsigned) r.reply.id);
        }
        return _amrmq_str_dup(buf);
    }
    return _amrmq_str_dup("unknown AMQP reply");
}

static void _amrmq_drop_envelope(AmalgameRabbitMQ* r) {
    if (r && r->have_envelope) {
        amqp_destroy_envelope(&r->envelope);
        r->have_envelope = 0;
    }
}

static void _amrmq_finalize(void* obj, void* cd) {
    (void) cd;
    AmalgameRabbitMQ* r = (AmalgameRabbitMQ*) obj;
    if (!r) return;
    _amrmq_drop_envelope(r);
    if (r->conn) {
        amqp_channel_close(r->conn, AMALGAME_RMQ_CHANNEL, AMQP_REPLY_SUCCESS);
        amqp_connection_close(r->conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(r->conn);
        r->conn = NULL;
    }
}

/* ── Lifecycle ──────────────────────────────────────── */

static inline AmalgameRabbitMQ* Amalgame_Messaging_RabbitMQ_Connect(
        code_string host, i64 port, code_string user,
        code_string password, code_string vhost) {
    AmalgameRabbitMQ* r = (AmalgameRabbitMQ*) GC_MALLOC(sizeof(AmalgameRabbitMQ));
    r->conn              = NULL;
    r->sock              = NULL;
    r->last_error        = NULL;
    r->last_body         = (char*) "";
    r->last_rk           = (char*) "";
    r->last_exchange     = (char*) "";
    r->last_delivery_tag = 0;
    r->have_envelope     = 0;
    GC_register_finalizer(r, _amrmq_finalize, NULL, NULL, NULL);

    if (!host)     { r->last_error = _amrmq_str_dup("null host");     return r; }
    if (!user)     { r->last_error = _amrmq_str_dup("null user");     return r; }
    if (!password) { r->last_error = _amrmq_str_dup("null password"); return r; }
    if (!vhost)    vhost = (code_string) "/";

    amqp_connection_state_t conn = amqp_new_connection();
    if (!conn) { r->last_error = _amrmq_str_dup("amqp_new_connection failed"); return r; }

    amqp_socket_t* sock = amqp_tcp_socket_new(conn);
    if (!sock) {
        amqp_destroy_connection(conn);
        r->last_error = _amrmq_str_dup("amqp_tcp_socket_new failed");
        return r;
    }
    int rc = amqp_socket_open(sock, host, (int) port);
    if (rc != AMQP_STATUS_OK) {
        amqp_destroy_connection(conn);
        const char* msg = amqp_error_string2(rc);
        r->last_error = _amrmq_str_dup(msg ? msg : "amqp_socket_open failed");
        return r;
    }

    /* PLAIN SASL auth. channel_max=0 → server's default (2047);
     * frame_max=131072 → librabbitmq's default; heartbeat=0 →
     * disabled (v2 surface for tuning these). */
    amqp_rpc_reply_t login = amqp_login(conn, vhost,
                                         0, 131072, 0,
                                         AMQP_SASL_METHOD_PLAIN,
                                         user, password);
    if (login.reply_type != AMQP_RESPONSE_NORMAL) {
        r->last_error = _amrmq_reply_err(login);
        amqp_destroy_connection(conn);
        return r;
    }

    amqp_channel_open(conn, AMALGAME_RMQ_CHANNEL);
    amqp_rpc_reply_t cr = amqp_get_rpc_reply(conn);
    if (cr.reply_type != AMQP_RESPONSE_NORMAL) {
        r->last_error = _amrmq_reply_err(cr);
        amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
        amqp_destroy_connection(conn);
        return r;
    }

    r->conn = conn;
    r->sock = sock;
    return r;
}

static inline void Amalgame_Messaging_RabbitMQ_Close(AmalgameRabbitMQ* r) {
    if (!r || !r->conn) return;
    _amrmq_drop_envelope(r);
    amqp_channel_close(r->conn, AMALGAME_RMQ_CHANNEL, AMQP_REPLY_SUCCESS);
    amqp_connection_close(r->conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(r->conn);
    r->conn = NULL;
}

static inline code_bool Amalgame_Messaging_RabbitMQ_IsOpen(AmalgameRabbitMQ* r) {
    return (r && r->conn) ? 1 : 0;
}

static inline code_string Amalgame_Messaging_RabbitMQ_LastError(AmalgameRabbitMQ* r) {
    if (!r || !r->last_error) return (code_string) "";
    return r->last_error;
}

/* ── Topology ──────────────────────────────────────── */

static inline code_bool Amalgame_Messaging_RabbitMQ_DeclareExchange(
        AmalgameRabbitMQ* r, code_string name, code_string type) {
    if (!r || !r->conn) { if (r) r->last_error = _amrmq_str_dup("not connected"); return 0; }
    if (!name || !type) { r->last_error = _amrmq_str_dup("null name or type"); return 0; }
    amqp_exchange_declare(r->conn, AMALGAME_RMQ_CHANNEL,
                          amqp_cstring_bytes(name),
                          amqp_cstring_bytes(type),
                          0, 0, 0, 0,
                          amqp_empty_table);
    amqp_rpc_reply_t rep = amqp_get_rpc_reply(r->conn);
    if (rep.reply_type != AMQP_RESPONSE_NORMAL) {
        r->last_error = _amrmq_reply_err(rep);
        return 0;
    }
    r->last_error = _amrmq_str_dup("");
    return 1;
}

/* Declare a queue. Empty name → server auto-names; the returned
 * string carries the generated name. Non-empty name returned as-is.
 *
 * AMQP 0.9.1 says auto-named queues are conventionally exclusive
 * (only their declaring connection can use them) — RabbitMQ 4+
 * enforces this strictly and closes the connection with PRECONDITION_FAILED
 * if a server-named queue is declared with exclusive=false. We
 * flip exclusive=1 automatically when name is empty so users
 * don't trip on that mismatch. Named queues stay non-exclusive
 * so worker pools / persistent consumers still work. */
static inline code_string Amalgame_Messaging_RabbitMQ_DeclareQueue(
        AmalgameRabbitMQ* r, code_string name) {
    if (!r || !r->conn) { if (r) r->last_error = _amrmq_str_dup("not connected"); return (code_string) ""; }
    if (!name) name = (code_string) "";
    int server_named = (*name == '\0') ? 1 : 0;
    int exclusive = server_named ? 1 : 0;
    int auto_delete = 1;
    amqp_queue_declare_ok_t* ok = amqp_queue_declare(
        r->conn, AMALGAME_RMQ_CHANNEL,
        amqp_cstring_bytes(name),
        0,             /* passive — create if not exists */
        0,             /* durable — false in v1 */
        exclusive,
        auto_delete,
        amqp_empty_table);
    amqp_rpc_reply_t rep = amqp_get_rpc_reply(r->conn);
    if (rep.reply_type != AMQP_RESPONSE_NORMAL || !ok) {
        r->last_error = _amrmq_reply_err(rep);
        return (code_string) "";
    }
    r->last_error = _amrmq_str_dup("");
    return _amrmq_bytes_dup(ok->queue);
}

static inline code_bool Amalgame_Messaging_RabbitMQ_Bind(
        AmalgameRabbitMQ* r, code_string queue,
        code_string exchange, code_string routingKey) {
    if (!r || !r->conn) { if (r) r->last_error = _amrmq_str_dup("not connected"); return 0; }
    if (!queue || !exchange) { r->last_error = _amrmq_str_dup("null queue/exchange"); return 0; }
    if (!routingKey) routingKey = (code_string) "";
    amqp_queue_bind(r->conn, AMALGAME_RMQ_CHANNEL,
                    amqp_cstring_bytes(queue),
                    amqp_cstring_bytes(exchange),
                    amqp_cstring_bytes(routingKey),
                    amqp_empty_table);
    amqp_rpc_reply_t rep = amqp_get_rpc_reply(r->conn);
    if (rep.reply_type != AMQP_RESPONSE_NORMAL) {
        r->last_error = _amrmq_reply_err(rep);
        return 0;
    }
    r->last_error = _amrmq_str_dup("");
    return 1;
}

/* ── Publish ───────────────────────────────────────── */

static inline code_bool Amalgame_Messaging_RabbitMQ_Publish(
        AmalgameRabbitMQ* r, code_string exchange,
        code_string routingKey, code_string body) {
    if (!r || !r->conn) { if (r) r->last_error = _amrmq_str_dup("not connected"); return 0; }
    if (!exchange) exchange = (code_string) "";   /* "" = default exchange */
    if (!routingKey) routingKey = (code_string) "";
    if (!body) body = (code_string) "";
    amqp_basic_properties_t props = {0};
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type   = amqp_cstring_bytes("text/plain");
    props.delivery_mode  = 1;   /* transient — v1 doesn't expose persistent yet */
    int rc = amqp_basic_publish(r->conn, AMALGAME_RMQ_CHANNEL,
                                 amqp_cstring_bytes(exchange),
                                 amqp_cstring_bytes(routingKey),
                                 0, 0,                            /* mandatory, immediate */
                                 &props,
                                 amqp_cstring_bytes(body));
    if (rc != AMQP_STATUS_OK) {
        const char* msg = amqp_error_string2(rc);
        r->last_error = _amrmq_str_dup(msg ? msg : "amqp_basic_publish failed");
        return 0;
    }
    r->last_error = _amrmq_str_dup("");
    return 1;
}

/* ── Consume ───────────────────────────────────────── */

/* Start the consumer on `queue`. Subsequent Poll calls fetch
 * delivered messages. consumer_tag is server-generated (empty
 * client tag), no_local / no_ack / exclusive all defaulted to 0. */
static inline code_bool Amalgame_Messaging_RabbitMQ_Consume(
        AmalgameRabbitMQ* r, code_string queue) {
    if (!r || !r->conn) { if (r) r->last_error = _amrmq_str_dup("not connected"); return 0; }
    if (!queue) { r->last_error = _amrmq_str_dup("null queue"); return 0; }
    amqp_basic_consume(r->conn, AMALGAME_RMQ_CHANNEL,
                       amqp_cstring_bytes(queue),
                       amqp_empty_bytes,                    /* consumer_tag */
                       0, 0, 0,                              /* no_local, no_ack, exclusive */
                       amqp_empty_table);
    amqp_rpc_reply_t rep = amqp_get_rpc_reply(r->conn);
    if (rep.reply_type != AMQP_RESPONSE_NORMAL) {
        r->last_error = _amrmq_reply_err(rep);
        return 0;
    }
    r->last_error = _amrmq_str_dup("");
    return 1;
}

/* Block for up to timeout_ms waiting for one message. Returns
 * true on delivery (last_* fields populated), false on timeout
 * / error / channel-close. Frees the previous envelope before
 * grabbing the next so memory doesn't accumulate. */
static inline code_bool Amalgame_Messaging_RabbitMQ_Poll(
        AmalgameRabbitMQ* r, i64 timeout_ms) {
    if (!r || !r->conn) { if (r) r->last_error = _amrmq_str_dup("not connected"); return 0; }

    _amrmq_drop_envelope(r);
    amqp_maybe_release_buffers(r->conn);

    struct timeval tv;
    struct timeval* tvp = NULL;
    if (timeout_ms > 0) {
        tv.tv_sec  = (long) (timeout_ms / 1000);
        tv.tv_usec = (long) ((timeout_ms % 1000) * 1000);
        tvp = &tv;
    }
    amqp_rpc_reply_t rep = amqp_consume_message(r->conn, &r->envelope, tvp, 0);
    if (rep.reply_type != AMQP_RESPONSE_NORMAL) {
        if (rep.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION
            && rep.library_error == AMQP_STATUS_TIMEOUT) {
            /* Timeout — not an error, no message yet. */
            return 0;
        }
        r->last_error = _amrmq_reply_err(rep);
        return 0;
    }

    r->have_envelope     = 1;
    r->last_body         = (char*) _amrmq_bytes_dup(r->envelope.message.body);
    r->last_rk           = (char*) _amrmq_bytes_dup(r->envelope.routing_key);
    r->last_exchange     = (char*) _amrmq_bytes_dup(r->envelope.exchange);
    r->last_delivery_tag = (i64) r->envelope.delivery_tag;
    r->last_error        = _amrmq_str_dup("");
    return 1;
}

static inline code_string Amalgame_Messaging_RabbitMQ_LastBody(AmalgameRabbitMQ* r) {
    if (!r || !r->last_body) return (code_string) "";
    return r->last_body;
}
static inline code_string Amalgame_Messaging_RabbitMQ_LastRoutingKey(AmalgameRabbitMQ* r) {
    if (!r || !r->last_rk) return (code_string) "";
    return r->last_rk;
}
static inline code_string Amalgame_Messaging_RabbitMQ_LastExchange(AmalgameRabbitMQ* r) {
    if (!r || !r->last_exchange) return (code_string) "";
    return r->last_exchange;
}
static inline i64 Amalgame_Messaging_RabbitMQ_LastDeliveryTag(AmalgameRabbitMQ* r) {
    return r ? r->last_delivery_tag : 0;
}

/* Acknowledge the last polled message. Multi-ack not exposed in
 * v1 — caller acks one at a time. */
static inline code_bool Amalgame_Messaging_RabbitMQ_Ack(AmalgameRabbitMQ* r) {
    if (!r || !r->conn) { if (r) r->last_error = _amrmq_str_dup("not connected"); return 0; }
    if (r->last_delivery_tag == 0) { r->last_error = _amrmq_str_dup("Ack: nothing to ack"); return 0; }
    int rc = amqp_basic_ack(r->conn, AMALGAME_RMQ_CHANNEL,
                             (uint64_t) r->last_delivery_tag, 0 /* not multiple */);
    if (rc != AMQP_STATUS_OK) {
        const char* msg = amqp_error_string2(rc);
        r->last_error = _amrmq_str_dup(msg ? msg : "amqp_basic_ack failed");
        return 0;
    }
    r->last_error = _amrmq_str_dup("");
    return 1;
}

#endif /* AMALGAME_MESSAGING_RABBITMQ_H */
