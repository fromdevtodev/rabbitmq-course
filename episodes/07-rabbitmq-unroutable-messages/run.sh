#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh experiment1     the silent drop: a key with no binding, published the normal way
#   ./run.sh experiment2     the mandatory flag: the same message comes back to the publisher
#   ./run.sh experiment3     the alternate exchange: the same message lands in a queue of your choice
#   ./run.sh counters        print how many messages each queue holds
#   ./run.sh bindings        print the bindings of the exchange orders
#   ./run.sh policy          attach the alternate exchange orders.unrouted to orders (a policy)
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app publish --key order.canceled --mandatory true
#   ./run.sh reset           delete both exchanges, the four queues and the policy
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from nothing, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

ORDER_QUEUES=(notifications.email loyalty.points audit.log)
ALL_QUEUES=(notifications.email loyalty.points audit.log unrouted.parking)
POLICY_NAME=AE

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }
ctl() { docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers "$@" 2>/dev/null; }

# "queue  ready" for the queues given as arguments; a queue that does not exist yet shows a dash
counters_of() {
  printf '%-20s %s\n' queue ready
  local q
  for q in "$@"; do
    printf '%-20s %s\n' "$q" "$(ctl list_queues name messages_ready | awk -v q="$q" '$1 == q { found = 1; print $2 } END { if (!found) print "-" }')"
  done
}

# The queues of the project; only the ones that exist are shown.
counters() {
  printf '%-20s %s\n' queue ready
  ctl list_queues name messages_ready | awk -v want="${ALL_QUEUES[*]}" \
    'BEGIN { n = split(want, w, " "); for (i = 1; i <= n; i++) mine[w[i]] = 1 } $1 in mine { printf "%-20s %s\n", $1, $2 }'
}

# The bindings of the exchange orders. An exact match: orders.unrouted also starts with "orders".
bindings() {
  printf '%-8s %-20s %s\n' exchange queue key
  ctl list_bindings source_name destination_name routing_key | awk -F'\t' '$1 == "orders" { printf "%-8s %-20s %s\n", $1, $2, $3 }'
}

# The alternate exchange is a property of orders, set by a policy: no change in the code, no redeploy.
policy() {
  ctl set_policy "$POLICY_NAME" "^orders$" '{"alternate-exchange":"orders.unrouted"}' --apply-to exchanges
}

# A policy outlives the exchange it applied to, so every fresh start clears it.
fresh() { build; app reset; ctl clear_policy "$POLICY_NAME" >/dev/null || true; app "$1"; }

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  experiment1)
    fresh setup
    say "1/3  The exchange orders has three queues; order.created and order.cancelled are bound; all queues are empty"
    bindings
    counters_of "${ORDER_QUEUES[@]}"
    say "2/3  evt_0001 has routing key order.created: three bindings match it"
    app publish --key order.created --first 1
    counters_of "${ORDER_QUEUES[@]}"
    say "3/3  evt_0002 has routing key order.canceled (one L): no binding has this key; the publish raises no error"
    app publish --key order.canceled --first 2
    counters_of "${ORDER_QUEUES[@]}"
    echo
    echo "The call returned normally and no queue received the message: it was dropped, and nothing says so."
    ;;
  experiment2)
    fresh setup
    say "1/3  Same exchange, all queues empty"
    bindings
    counters_of "${ORDER_QUEUES[@]}"
    say "2/3  evt_0001 has routing key order.canceled, published with the mandatory flag: the broker gives it back"
    app publish --key order.canceled --first 1 --mandatory true
    counters_of "${ORDER_QUEUES[@]}"
    say "3/3  evt_0002 has routing key order.created, also mandatory: it is routed, so nothing comes back"
    app publish --key order.created --first 2 --mandatory true
    counters_of "${ORDER_QUEUES[@]}"
    echo
    echo "The flag does not store the message anywhere: it only tells the publisher that it was not routed."
    ;;
  experiment3)
    fresh setup-parking
    say "1/4  Same exchange, plus orders.unrouted and the queue unrouted.parking, not attached yet"
    bindings
    counters_of "${ALL_QUEUES[@]}"
    say "2/4  Attach the alternate exchange to orders with a policy"
    policy
    ctl list_policies
    say "3/4  evt_0001 has routing key order.canceled: no binding matches, so the exchange passes it to orders.unrouted"
    app publish --key order.canceled --first 1
    counters_of "${ALL_QUEUES[@]}"
    say "4/4  evt_0002 has the same key and the mandatory flag; evt_0003 is routed normally"
    app publish --key order.canceled --first 2 --mandatory true
    app publish --key order.created --first 3
    counters_of "${ALL_QUEUES[@]}"
    echo
    echo "A message that goes to the alternate exchange counts as routed: the mandatory flag has nothing to return."
    ;;
  counters)
    counters
    ;;
  bindings)
    bindings
    ;;
  policy)
    policy
    ctl list_policies
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
    ctl clear_policy "$POLICY_NAME" >/dev/null || true
    ;;
  down)
    docker compose down -v
    ;;
  help | -h | --help)
    sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "Unknown command: $1 (try ./run.sh help)" >&2
    exit 2
    ;;
esac
