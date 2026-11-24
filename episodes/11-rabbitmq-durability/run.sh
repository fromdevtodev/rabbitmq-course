#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh experiment1     a durable queue and transient messages: three messages, a restart, and the queue comes back empty
#   ./run.sh experiment2     two durable queues, persistent messages in one and transient in the other: a restart keeps only one
#   ./run.sh counters        print the queues: durable or not, and how many messages each holds
#   ./run.sh restart         restart the broker the way a deploy does (waits for it to come back)
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app publish --count 3 --persistent true
#   ./run.sh reset           delete both queues
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from empty queues, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }
ctl() { docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers "$@" 2>/dev/null; }

# "queue  durable  messages" for the two queues of this episode
counters() {
  printf '%-22s %-8s %s\n' queue durable messages
  ctl list_queues name durable messages | awk -F'\t' '$1 == "notifications.email" || $1 == "loyalty.points" { printf "%-22s %-8s %s\n", $1, $2, $3 }'
}

# Waits until the broker answers again after a restart (up to two minutes).
wait_up() {
  for _ in $(seq 1 120); do
    ctl list_queues name >/dev/null && return 0
    sleep 1
  done
  echo "RabbitMQ did not come back. Check: docker compose logs rabbitmq" >&2
  return 1
}

# The same restart a deploy does: stop the container (up to 60 s for a clean shutdown) and start it again.
restart_broker() {
  docker compose restart -t 60 rabbitmq
  wait_up
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
    say "1/4  Two durable queues, both empty"
    counters
    say "2/4  The order service publishes three confirmations with no properties at all: transient messages"
    app publish --queue notifications.email --count 3 --first 1
    counters
    say "3/4  The broker restarts, the way it does in any deploy"
    restart_broker
    say "4/4  The queue is back and still durable, but it holds nothing: the publisher never saw an error"
    counters
    echo
    echo "A durable queue is recreated after a restart. Transient messages are not kept."
    ;;
  experiment2)
    fresh
    say "1/4  Two durable queues, both empty"
    counters
    say "2/4  Three persistent confirmations to notifications.email, three transient updates to loyalty.points"
    app publish --queue notifications.email --count 3 --first 1 --persistent true
    app publish --queue loyalty.points --count 3 --first 4
    counters
    say "3/4  The broker restarts"
    restart_broker
    say "4/4  Both queues are back; only the persistent messages are"
    counters
    echo
    echo "Same queue setting, different message setting, different result."
    ;;
  counters)
    counters
    ;;
  restart)
    restart_broker
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
