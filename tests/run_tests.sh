#!/bin/bash
# ─────────────────────────────────────────────────────
#  amalgame-messaging-rabbitmq — Test Runner
#  Usage: ./tests/run_tests.sh [/path/to/amc]
#
#  Double-gated:
#    1. librabbitmq-dev installed
#    2. RabbitMQ broker reachable at $RABBITMQ_HOST:5672
#       (default 127.0.0.1:5672, default guest/guest auth)
#
#  Start a broker locally:
#    docker run --rm -d -p 5672:5672 rabbitmq:4
# ─────────────────────────────────────────────────────

set -u

if [ $# -ge 1 ]; then
    AMC="$1"
elif [ -n "${AMC:-}" ]; then
    :
elif command -v amc >/dev/null 2>&1; then
    AMC="$(command -v amc)"
else
    echo "ERROR: amc not found." >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PKG_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PKG_RUNTIME="$PKG_ROOT/runtime"

AMC_DIR="$(cd "$(dirname "$AMC")" && pwd)"
if [ -d "$AMC_DIR/runtime" ]; then
    AMC_RUNTIME="$AMC_DIR/runtime"
elif [ -d "$AMC_DIR/../share/amalgame/runtime" ]; then
    AMC_RUNTIME="$AMC_DIR/../share/amalgame/runtime"
elif [ -n "${AMC_RUNTIME:-}" ]; then
    :
else
    echo "ERROR: amc runtime/ not found. Set AMC_RUNTIME=..." >&2
    exit 2
fi

BUILD_DIR="$(mktemp -d -t armq-XXXXXX)"
trap 'rm -rf "$BUILD_DIR"' EXIT
PROJ_DIR="$BUILD_DIR/proj"
mkdir -p "$PROJ_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'
PASS=0; FAIL=0; SKIP=0

echo ""
echo "════════════════════════════════════════════"
echo "  amalgame-messaging-rabbitmq — Tests"
echo "════════════════════════════════════════════"
echo "  amc:     $AMC ($("$AMC" --version 2>&1 | head -1))"
echo "  runtime: $AMC_RUNTIME"
echo ""

# ── Gate 1 : librabbitmq-dev present? ───────────────
echo "── Probing librabbitmq ─────────────────────"
RMQ_OK=0
for h in 'rabbitmq-c/amqp.h' 'amqp.h'; do
    echo "#include <$h>" > "$BUILD_DIR/_inc.c"
    if gcc -E "$BUILD_DIR/_inc.c" >/dev/null 2>&1; then
        RMQ_OK=1
        echo "  $h: found"
        break
    fi
done
if [ "$RMQ_OK" = "0" ]; then
    echo "  amqp.h: NOT FOUND (install librabbitmq-dev / librabbitmq-devel / rabbitmq-c)"
fi
echo ""

# ── Gate 2 : RabbitMQ broker reachable? ─────────────
RABBITMQ_HOST="${RABBITMQ_HOST:-127.0.0.1}"
RABBITMQ_PORT="${RABBITMQ_PORT:-5672}"
RMQ_REACH=0
if (echo > /dev/tcp/$RABBITMQ_HOST/$RABBITMQ_PORT) 2>/dev/null; then
    RMQ_REACH=1
    echo "  rabbitmq:        reachable at $RABBITMQ_HOST:$RABBITMQ_PORT"
else
    echo "  rabbitmq:        NOT reachable at $RABBITMQ_HOST:$RABBITMQ_PORT"
fi
echo ""

# ── Stage fake cache for the test fixture ───────────
FAKE_CACHE="$BUILD_DIR/cache"
PKG_GIT="github.com/amalgame-lang/amalgame-messaging-rabbitmq"
PKG_TAG="${PKG_TAG:-v0.1.0}"
FAKE_SHA="deadbeefcafebabe0000000000000000000000ab"
SHORT_SHA="${FAKE_SHA:0:8}"
PKG_CACHE_DIR="$FAKE_CACHE/$PKG_GIT/${PKG_TAG}_${SHORT_SHA}"

mkdir -p "$(dirname "$PKG_CACHE_DIR")"
rm -rf "$PKG_CACHE_DIR"
ln -s "$PKG_ROOT" "$PKG_CACHE_DIR"

cat > "$PROJ_DIR/amalgame.lock" <<EOF
[[package]]
name = "amalgame-messaging-rabbitmq"
git  = "$PKG_GIT"
tag  = "$PKG_TAG"
rev  = "$FAKE_SHA"
EOF

export AMALGAME_PACKAGES_DIR="$FAKE_CACHE"

run_test() {
    local name="$1"
    local expected="$2"
    printf "  %-40s" "$name"
    if [ "$RMQ_OK" = "0" ]; then
        echo -e "${YELLOW}SKIP${NC} (librabbitmq-dev not installed)"
        SKIP=$((SKIP + 1)); return
    fi
    if [ "$RMQ_REACH" = "0" ]; then
        echo -e "${YELLOW}SKIP${NC} (no rabbitmq at $RABBITMQ_HOST:$RABBITMQ_PORT)"
        SKIP=$((SKIP + 1)); return
    fi
    cp "$SCRIPT_DIR/stdlib_rabbitmq.am" "$PROJ_DIR/test.am"
    local out_base="$PROJ_DIR/test"
    local out
    out=$(cd "$PROJ_DIR" && "$AMC" -o test test.am 2>&1)
    if [ $? -ne 0 ]; then
        echo -e "${RED}FAIL${NC} (amc)"; echo "$out" | head -3 | sed 's/^/    /'
        FAIL=$((FAIL + 1)); return
    fi
    if [ ! -f "$out_base.c" ]; then
        echo -e "${RED}FAIL${NC} (no .c)"; FAIL=$((FAIL + 1)); return
    fi
    gcc -O2 -w \
        -I"$AMC_RUNTIME" -I"$PKG_RUNTIME" \
        "$out_base.c" \
        -lgc -lm -lcurl -ldl -lpthread -lrabbitmq \
        -o "$out_base" 2>"$BUILD_DIR/link.log"
    if [ ! -x "$out_base" ]; then
        echo -e "${RED}FAIL${NC} (gcc link)"
        cat "$BUILD_DIR/link.log" | head -3 | sed 's/^/    /'
        FAIL=$((FAIL + 1)); return
    fi
    local run_output
    run_output=$("$out_base" 2>&1)
    if echo "$run_output" | grep -qF "$expected"; then
        echo -e "${GREEN}PASS${NC}"; PASS=$((PASS + 1))
    else
        echo -e "${RED}FAIL${NC}"
        echo "    expected: $expected"
        echo "    got:      $(echo "$run_output" | head -3 | tr '\n' '|')"
        FAIL=$((FAIL + 1))
    fi
}

echo "── RabbitMQ ────────────────────────────────"
run_test "Connect"                       "[PASS] Connect"
run_test "DeclareExchange direct"        "[PASS] DeclareExchange direct"
run_test "DeclareQueue server-named"     "[PASS] DeclareQueue server-named"
run_test "Bind"                          "[PASS] Bind"
run_test "Publish"                       "[PASS] Publish"
run_test "Consume"                       "[PASS] Consume"
run_test "Poll received message"         "[PASS] Poll received message"
run_test "LastBody matches"              "[PASS] LastBody matches"
run_test "LastRoutingKey matches"        "[PASS] LastRoutingKey matches"
run_test "LastExchange matches"          "[PASS] LastExchange matches"
run_test "LastDeliveryTag > 0"           "[PASS] LastDeliveryTag > 0"
run_test "Ack"                           "[PASS] Ack"
run_test "Close"                         "[PASS] Close"

echo ""
echo "────────────────────────────────────────────"
echo -e "  ${GREEN}PASS: $PASS${NC}  |  ${RED}FAIL: $FAIL${NC}  |  ${YELLOW}SKIP: $SKIP${NC}"
echo "────────────────────────────────────────────"
echo ""

[ $FAIL -eq 0 ] && exit 0 || exit 1
