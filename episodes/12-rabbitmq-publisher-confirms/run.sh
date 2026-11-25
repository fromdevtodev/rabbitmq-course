#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh experiment1     a queue that holds two messages: two confirms and a nack, with nobody consuming
#   ./run.sh experiment2     a frozen broker: no answer for message 2, a republish, and three messages for two orders
#   ./run.sh counters        print the queue: ready messages and consumers
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app publish --count 5 --pace burst
#   ./run.sh reset           delete the queue
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from an empty queue, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }
ctl() { docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers "$@" 2>/dev/null; }

# "queue  ready  consumers" for the queue of this episode
counters() {
  printf '%-12s %-8s %s\n' queue ready consumers
  ctl list_queues name messages_ready consumers | awk -F'\t' '$1 == "orders.in" { printf "%-12s %-8s %s\n", $1, $2, $3 }'
}

# Waits until a text appears in a file (up to 60 seconds).
wait_for() {
  for _ in $(seq 1 600); do
    grep -qF -- "$1" "$2" 2>/dev/null && return 0
    sleep 0.1
  done
  echo "Timed out waiting for: $1" >&2
  return 1
}

fresh() { build; app reset; app setup --limit "$1"; }

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  experiment1)
    fresh 2
    say "1/3  The queue is durable and holds at most two messages. Nobody consumes from it"
    counters
    say "2/3  Three persistent messages, one at a time: the producer waits for each answer"
    app publish --count 3 --pace one-by-one
    say "3/3  Two were confirmed and nobody has read them. The third was refused: the queue was full"
    counters
    echo
    echo "A confirm says the broker took the message. It does not say that anyone processed it."
    ;;
  experiment2)
    fresh none
    log="$(mktemp)"
    go="target/go-ahead"; mkdir -p target; rm -f "$go"
    trap 'docker compose unpause rabbitmq >/dev/null 2>&1 || true; rm -f "$log" "$go"' EXIT
    say "1/4  The queue has no limit. The producer publishes event 1 and waits for its confirm"
    app republish --gate "$go" --timeout-ms 3000 | tee "$log" &
    producer=$!
    wait_for "#1 confirmed" "$log"
    say "2/4  Freeze the broker (docker compose pause), then the producer publishes event 2"
    docker compose pause rabbitmq
    : > "$go"
    wait_for "publishing evt_0002 again" "$log"
    sleep 1
    say "3/4  Unfreeze the broker: the answers arrive"
    docker compose unpause rabbitmq
    wait "$producer"
    say "4/4  Three messages for two orders: event 2 is in the queue twice"
    counters
    echo
    echo "No answer is not a nack and not a loss: it is an unknown. Publishing again is safe for the message and creates a duplicate."
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
