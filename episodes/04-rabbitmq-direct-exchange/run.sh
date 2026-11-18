#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh experiment1     one key, three bindings: which queues get a copy of error, warning and debug
#   ./run.sh experiment2     the default exchange: the binding nobody made, a typo, and a binding that is refused
#   ./run.sh counters        print how many messages each queue holds
#   ./run.sh bindings        print the bindings of the exchange logs
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app publish --key info --first 9
#   ./run.sh reset           delete the exchange and the four queues
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

# "queue  ready" for the four queues of this episode; a queue that does not exist yet shows a dash
counters() {
  printf '%-22s %s\n' queue ready
  for q in errors-pager audit metrics notifications.email; do
    printf '%-22s %s\n' "$q" "$(ctl list_queues name messages_ready | awk -v q="$q" '$1 == q { found = 1; print $2 } END { if (!found) print "-" }')"
  done
}

# The bindings of the exchange logs. Rows with another source name belong to other exchanges (the default exchange, for one).
bindings() {
  printf '%-8s %-14s %s\n' exchange queue binding_key
  ctl list_bindings source_name destination_name routing_key | awk -F'\t' '$1 == "logs" { printf "%-8s %-14s %s\n", $1, $2, $3 }'
}

# The bindings that the broker itself made: an empty exchange name is the default exchange. It is shown as "".
default_bindings() {
  printf '%-8s %-22s %s\n' exchange queue binding_key
  ctl list_bindings source_name destination_name routing_key |
    awk -F'\t' '$1 == "" && $2 == "notifications.email" { printf "%-8s %-22s %s\n", "\"\"", $2, $3 }'
}

fresh1() { build; app reset; app setup; }
fresh2() { build; app reset; app declare; }

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  experiment1)
    fresh1
    say "1/4  The exchange logs has three queues and five bindings; all queues are empty"
    bindings
    counters
    say "2/4  log_0001 has routing key error: errors-pager and audit have a binding with the key error"
    app publish --key error --first 1
    counters
    say "3/4  log_0002 has routing key warning: audit and metrics. Audit is bound three times and still gets one copy"
    app publish --key warning --first 2
    counters
    say "4/4  log_0003 has routing key debug: no binding has this key, so no queue gets a copy and the publish does not fail"
    app publish --key debug --first 3
    counters
    echo
    echo "Equal keys get a copy; a key that matches nothing delivers nothing."
    ;;
  experiment2)
    fresh2
    say "1/4  One queue and no binding made by us: the broker lists one anyway, from the default exchange"
    default_bindings
    counters
    say "2/4  Publish to the default exchange (the empty name) with the name of the queue as the routing key"
    app publish-default --queue notifications.email --first 1
    counters
    say "3/4  A typo in the routing key: no queue has this name, so nothing arrives and the publish does not fail"
    app publish-default --queue notifications.mail --first 2
    counters
    say "4/4  Try to add a binding of our own to the default exchange"
    app bind-default --queue notifications.email --key audit
    echo
    echo "The default exchange is a direct exchange with no name: every queue is bound to it with its own name."
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
