#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh experiment1     build the dead-letter exchange and queue, reject an order, find it in the dead-letter queue
#   ./run.sh experiment2     the same setup with the dead-letter queue bound with another key: the rejected order is gone
#   ./run.sh counters        print ready, unacknowledged and consumers of both queues
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app setup --max-length 2
#   ./run.sh reset           delete both queues and both exchanges
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from nothing, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }
ctl() { docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers "$@" 2>/dev/null; }

# "queue  ready  unacked  consumers" of the two queues of this episode; a queue that does not exist yet shows dashes
counters() {
  printf '%-26s %6s %8s %10s\n' queue ready unacked consumers
  local q
  for q in notifications.email notifications.email.dlq; do
    ctl list_queues name messages_ready messages_unacknowledged consumers |
      awk -v q="$q" '$1 == q { found = 1; printf "%-26s %6s %8s %10s\n", $1, $2, $3, $4 } END { if (!found) printf "%-26s %6s %8s %10s\n", q, "-", "-", "-" }'
  done
}

fresh() { build; app reset; }

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  experiment1)
    fresh
    say "1/6  Nothing exists yet"
    counters
    say "2/6  Build step 1: the dead-letter exchange orders.dlx and its queue"
    app setup --stage dead-letter
    say "3/6  Build step 2: the work queue, which names orders.dlx"
    app setup --stage source
    counters
    say "4/6  Three orders: evt_0001, evt_0002 (no email address) and evt_0003"
    app publish --first 1 --count 3 --bad 2
    say "5/6  The worker rejects evt_0002 with requeue=false; it stops itself after 3 deliveries"
    app work --work-ms 300 --max-deliveries 3
    counters
    say "6/6  Why did evt_0002 die? Read the x-death header"
    app inspect
    ;;
  experiment2)
    fresh
    say "1/4  The dead-letter queue is bound with order.failed, but a dead letter keeps the key order.created"
    app setup --stage dead-letter --dlq-key order.failed
    app setup --stage source
    counters
    say "2/4  The same three orders"
    app publish --first 1 --count 3 --bad 2
    say "3/4  The worker rejects evt_0002 with requeue=false; it stops itself after 3 deliveries"
    app work --work-ms 300 --max-deliveries 3
    say "4/4  Both queues are empty: nothing kept evt_0002, and nothing reported an error"
    counters
    app inspect
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
