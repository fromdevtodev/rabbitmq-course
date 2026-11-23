#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker and declare the queue
#   ./run.sh hoarding        experiment 1: prefetch 0 (no limit), a fast and a slow worker, 80 orders
#   ./run.sh fair-dispatch   experiment 2: the same, with prefetch 1
#   ./run.sh pile-returns    experiment 3: prefetch 10, one slow worker, then it is killed: the whole pile returns
#   ./run.sh counters        print the queue's counters once
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app work --name fast --delay-ms 50 --prefetch 5
#   ./run.sh reset           delete the queue
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from an empty queue, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUEUE="notifications.email"
ORDERS=80
FAST_MS=50    # 20 messages per second
SLOW_MS=500   # 2 messages per second

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }

# "queue  ready  unacked  consumers" for the one queue of this episode
counters() {
  printf 'queue                  ready  unacked  consumers\n'
  docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers list_queues name messages_ready messages_unacknowledged consumers 2>/dev/null |
    awk -v q="$QUEUE" '$1 == q { printf "%-22s %5s  %7s  %9s\n", $1, $2, $3, $4 }'
}

consumers() {
  docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers list_queues name consumers 2>/dev/null |
    awk -v q="$QUEUE" '$1 == q { print $2 }'
}

# "ready unacked" as two numbers on one line
depth() {
  docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers list_queues name messages_ready messages_unacknowledged 2>/dev/null |
    awk -v q="$QUEUE" '$1 == q { print $2, $3 }'
}

fresh() { build; app reset; app setup; }

# usage: split <prefetch>
# Starts both workers first, then publishes ORDERS messages at once, prints the counters until the queue is empty and
# shows how many messages each worker took.
split() {
  local prefetch="$1" log_fast log_slow pids=()
  log_fast="$(mktemp)"; log_slow="$(mktemp)"
  trap 'for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; rm -f "$log_fast" "$log_slow"' EXIT
  fresh
  say "1/3  Two workers join: fast (${FAST_MS} ms per message) and slow (${SLOW_MS} ms per message), prefetch $prefetch"
  mvn -q exec:java -Dexec.args="work --name fast --delay-ms $FAST_MS --prefetch $prefetch" > "$log_fast" 2>&1 &
  pids+=("$!")
  mvn -q exec:java -Dexec.args="work --name slow --delay-ms $SLOW_MS --prefetch $prefetch" > "$log_slow" 2>&1 &
  pids+=("$!")
  for _ in $(seq 1 40); do [[ "$(consumers)" == "2" ]] && break; sleep 1; done
  counters
  say "2/3  The producer publishes $ORDERS orders at once. Watch ready, unacked and the time it takes"
  app publish --count "$ORDERS"
  local started="$SECONDS" d
  while true; do
    counters | tail -n 1
    d="$(depth)"
    [[ "$d" == "0 0" ]] && break
    (( SECONDS - started > 60 )) && { echo "(still working after 60 s; stopping the watch)"; break; }
    sleep 1
  done
  say "3/3  How the $ORDERS orders were split (the queue took about $((SECONDS - started)) s to drain)"
  printf 'fast handled %s\n' "$(grep -c ' handled ' "$log_fast" || true)"
  printf 'slow handled %s\n' "$(grep -c ' handled ' "$log_slow" || true)"
}

# Experiment 3: one slow worker (20 s per order) with prefetch 10 holds ten of twenty orders; it is killed and they all return.
pile_returns() {
  local worker
  fresh
  trap 'kill -9 "${worker:-}" 2>/dev/null || true' EXIT
  say "1/3  One slow worker (20 s per order) joins with prefetch 10"
  mvn -q exec:java -Dexec.args="work --name slow --delay-ms 20000 --prefetch 10" &
  worker=$!
  for _ in $(seq 1 40); do [[ "$(consumers)" == "1" ]] && break; sleep 1; done
  counters
  say "2/3  The producer publishes 20 orders: the worker holds ten and works on one"
  app publish --count 20
  for _ in $(seq 1 30); do [[ "$(depth)" == "10 10" ]] && break; sleep 1; done
  counters
  say "3/3  The worker crashes (kill -9: no acknowledgement, no clean shutdown)"
  kill -9 "$worker" 2>/dev/null || true
  wait "$worker" 2>/dev/null || true
  for _ in $(seq 1 30); do [[ "$(depth)" == "20 0" ]] && break; sleep 1; done
  counters
}

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    app setup
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  hoarding)
    split 0
    echo
    echo "With no limit the broker dealt the orders out in turn, so the slow worker still holds its share."
    ;;
  fair-dispatch)
    split 1
    echo
    echo "With prefetch 1 a worker gets a new order only after it acknowledged the last one."
    ;;
  pile-returns)
    pile_returns
    echo
    echo "RabbitMQ saw the connection close and put the ten orders back: nothing was lost, and the half-done order will be delivered again."
    ;;
  counters)
    counters
    ;;
  watch)
    while true; do clear; counters; sleep 0.5; done
    ;;
  test)
    mvn -q test
    echo "Tests passed."
    ;;
  app)
    shift
    build
    app "$@"
    ;;
  reset)
    build
    app reset
    ;;
  down)
    docker compose down -v
    ;;
  help | -h | --help)
    sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "Unknown command: $1 (try ./run.sh help)" >&2
    exit 2
    ;;
esac
