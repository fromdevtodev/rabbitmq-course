#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker and declare the queue
#   ./run.sh experiment1     faster for whom?
#   ./run.sh experiment2     the provider is down
#   ./run.sh experiment3     a fast checkout, a growing queue
#   ./run.sh counters        print the queue's counters once
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app checkout --mode async --orders 5
#   ./run.sh reset           delete the queue
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from an empty queue, so you can repeat it.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUEUE="notifications.email"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }

# "name  messages_ready  consumers" for the one queue of this episode
counters() {
  printf 'queue                  ready  consumers\n'
  docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers list_queues name messages_ready consumers 2>/dev/null |
    awk -v q="$QUEUE" '$1 == q { printf "%-22s %5s  %9s\n", $1, $2, $3 }'
}

consumers() {
  docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers list_queues name consumers 2>/dev/null |
    awk -v q="$QUEUE" '$1 == q { print $2 }'
}

fresh() { build; app reset; app setup; }

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    app setup
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  experiment1)
    fresh
    say "1/3  Without a broker: each customer waits for the email provider (about 2 s each)"
    app checkout --mode sync --orders 3
    say "2/3  With a broker: the customers are answered at once, the messages wait in the queue"
    app checkout --mode async --orders 3
    counters
    say "3/3  The worker sends the three emails: 3 x 2 s of work, the same six seconds as before"
    app work --limit 3
    counters
    ;;
  experiment2)
    fresh
    say "1/3  Provider down, no broker: the order is saved, then the checkout fails"
    app checkout --mode sync --orders 1 --provider down
    say "2/3  Provider down, with a broker: all three customers are answered, three messages wait, no consumer"
    app checkout --mode async --orders 3 --provider down
    counters
    say "3/3  The provider is back: the worker drains the queue"
    app work --limit 3
    counters
    ;;
  experiment3)
    fresh
    worker_pid=""
    trap '[[ -z "$worker_pid" ]] || kill "$worker_pid" 2>/dev/null || true' EXIT
    say "1/3  Start a slow worker (3 s per email); it keeps running in the background"
    app work --provider-ms 3000 &
    worker_pid=$!
    for _ in $(seq 1 30); do [[ "$(consumers)" == "1" ]] && break; sleep 1; done
    [[ "$(consumers)" == "1" ]] || { echo "The worker did not connect." >&2; exit 1; }
    say "2/3  Twelve orders, one per second: every customer is answered fast"
    app checkout --mode async --orders 12 --interval-ms 1000
    say "3/3  ...and yet the queue is still full. Nothing logged an error."
    counters
    sleep 3
    counters
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
