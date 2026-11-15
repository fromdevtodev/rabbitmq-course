#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker and declare the exchange, notifications.email and audit.log
#   ./run.sh experiment      one publisher, a queue added while it runs, and the routing key deciding who gets a copy
#   ./run.sh counters        print how many messages each queue holds
#   ./run.sh bindings        print the bindings of the exchange orders
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app publish --key order.cancelled --first 9
#   ./run.sh reset           delete the exchange and the three queues
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

# "queue  ready" for the three queues of this episode; a queue that does not exist yet shows a dash
counters() {
  printf '%-22s %s\n' queue ready
  for q in notifications.email loyalty.points audit.log; do
    printf '%-22s %s\n' "$q" "$(ctl list_queues name messages_ready | awk -v q="$q" '$1 == q { found = 1; print $2 } END { if (!found) print "-" }')"
  done
}

# The bindings of the exchange orders. Rows with an empty source name belong to the built-in default exchange: they are filtered out.
bindings() {
  printf '%-10s %-22s %s\n' exchange queue routing_key
  ctl list_bindings source_name destination_name routing_key | awk -F'\t' '$1 == "orders" { printf "%-10s %-22s %s\n", $1, $2, $3 }'
}

fresh() { build; app reset; app setup; }

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    app setup
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  experiment)
    fresh
    say "1/5  The exchange orders has two queues bound to it, both empty; loyalty.points does not exist yet"
    bindings
    counters
    say "2/5  order-service publishes evt_0001 with routing key order.created: it names the exchange, not a queue"
    app publish --key order.created --first 1
    counters
    say "3/5  The points service adds its queue and binds it to orders. order-service is not touched"
    app bind-points
    bindings
    counters
    say "4/5  The same publisher sends evt_0002 (order.created): one publish, two copies"
    app publish --key order.created --first 2
    counters
    say "5/5  evt_0003 has routing key order.cancelled: only audit.log has a binding that matches"
    app publish --key order.cancelled --first 3
    counters
    echo
    echo "A binding only affects messages published after it exists: loyalty.points never got evt_0001."
    ;;
  counters)
    counters
    ;;
  bindings)
    bindings
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
