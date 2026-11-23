#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh experiment1     three orders, one with no email address, and a worker that nacks it with requeue true: the loop
#   ./run.sh experiment2     the same three orders, and a worker that nacks it with requeue false: the queue moves on
#   ./run.sh counters        print ready, unacknowledged and consumers of the queue notifications.email
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app work --on-invalid discard
#   ./run.sh reset           delete the exchange and the queue
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from nothing, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUEUE=notifications.email

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }
ctl() { docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers "$@" 2>/dev/null; }

# "queue  ready  unacked  consumers" of the queue of this episode; a queue that does not exist yet shows dashes
counters() {
  printf '%-20s %6s %8s %10s\n' queue ready unacked consumers
  ctl list_queues name messages_ready messages_unacknowledged consumers |
    awk -v q="$QUEUE" '$1 == q { found = 1; printf "%-20s %6s %8s %10s\n", $1, $2, $3, $4 } END { if (!found) printf "%-20s %6s %8s %10s\n", q, "-", "-", "-" }'
}

fresh() { build; app reset; app setup; }

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  experiment1)
    fresh
    say "1/4  The queue is empty and no worker is running"
    counters
    say "2/4  Three orders: evt_0001, evt_0002 (no email address) and evt_0003"
    app publish --first 1 --count 3 --bad 2
    counters
    say "3/4  The worker nacks an invalid order with requeue=true; it stops itself after 8 deliveries"
    app work --on-invalid requeue --work-ms 700 --max-deliveries 8
    say "4/4  Back in the queue: evt_0002 at the front, evt_0003 behind it, and no email was ever sent for either"
    counters
    echo
    echo "Count the lines 'received evt_0002' above, and look for 'received evt_0003': there is none."
    ;;
  experiment2)
    fresh
    say "1/3  The queue is empty and no worker is running"
    counters
    say "2/3  The same three orders"
    app publish --first 1 --count 3 --bad 2
    counters
    say "3/3  The worker nacks an invalid order with requeue=false; it stops itself after 3 deliveries"
    app work --on-invalid discard --work-ms 300 --max-deliveries 3
    counters
    echo
    echo "evt_0002 was received once and is gone: no dead-letter exchange is configured, so nothing kept it."
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
    sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "Unknown command: $1 (try ./run.sh help)" >&2
    exit 2
    ;;
esac
