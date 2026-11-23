#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh experiment1     four patterns, three keys: which queues get a copy of each key
#   ./run.sh experiment2     star against hash, and a star inside a word, on the keys orders, orders.eu, orders.eu.created
#   ./run.sh counters        print how many messages each queue holds
#   ./run.sh bindings        print the bindings of the exchange events
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app publish --key orders.eu.shipped --first 9
#   ./run.sh reset           delete the exchange and the seven queues
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from nothing, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

ROUTING_QUEUES=(created-orders all-orders eu-audit payments)
COMPARISON_QUEUES=(orders-star orders-hash orders-glob)

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }
ctl() { docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers "$@" 2>/dev/null; }

# "queue  ready" for the queues given as arguments; a queue that does not exist yet shows a dash
counters_of() {
  printf '%-16s %s\n' queue ready
  local q
  for q in "$@"; do
    printf '%-16s %s\n' "$q" "$(ctl list_queues name messages_ready | awk -v q="$q" '$1 == q { found = 1; print $2 } END { if (!found) print "-" }')"
  done
}

# The seven queues of the project; only the ones that exist are shown.
counters() {
  printf '%-16s %s\n' queue ready
  ctl list_queues name messages_ready | awk -v want="${ROUTING_QUEUES[*]} ${COMPARISON_QUEUES[*]}" \
    'BEGIN { n = split(want, w, " "); for (i = 1; i <= n; i++) mine[w[i]] = 1 } $1 in mine { printf "%-16s %s\n", $1, $2 }'
}

# The bindings of the exchange events. Rows with another source name belong to other exchanges (the default exchange, for one).
bindings() {
  printf '%-8s %-16s %s\n' exchange queue pattern
  ctl list_bindings source_name destination_name routing_key | awk -F'\t' '$1 == "events" { printf "%-8s %-16s %s\n", $1, $2, $3 }'
}

fresh1() { build; app reset; app setup; }
fresh2() { build; app reset; app setup-comparison; }

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  experiment1)
    fresh1
    say "1/4  The exchange events has four queues and four patterns; all queues are empty"
    bindings
    counters_of "${ROUTING_QUEUES[@]}"
    say "2/4  evt_0001 has routing key orders.eu.created: three patterns match it, payments.# does not"
    app publish --key orders.eu.created --first 1
    counters_of "${ROUTING_QUEUES[@]}"
    say "3/4  evt_0002 has routing key orders.us.cancelled: only orders.# matches (the second word is us, the last is cancelled)"
    app publish --key orders.us.cancelled --first 2
    counters_of "${ROUTING_QUEUES[@]}"
    say "4/4  evt_0003 has routing key payments.eu.failed: *.eu.* and payments.# match it"
    app publish --key payments.eu.failed --first 3
    counters_of "${ROUTING_QUEUES[@]}"
    echo
    echo "A pattern matches whole words, one by one: star is exactly one word, hash is zero or more."
    ;;
  experiment2)
    fresh2
    say "1/4  Three queues, three patterns: orders.*, orders.# and ord*.eu.created; all queues are empty"
    bindings
    counters_of "${COMPARISON_QUEUES[@]}"
    say "2/4  evt_0001 has routing key orders (one word): hash can be zero words, star needs exactly one"
    app publish --key orders --first 1
    counters_of "${COMPARISON_QUEUES[@]}"
    say "3/4  evt_0002 has routing key orders.eu (two words): now star matches too"
    app publish --key orders.eu --first 2
    counters_of "${COMPARISON_QUEUES[@]}"
    say "4/4  evt_0003 has routing key orders.eu.created (three words): too many for one star; ord*.eu.created still matches nothing"
    app publish --key orders.eu.created --first 3
    counters_of "${COMPARISON_QUEUES[@]}"
    echo
    echo "Star is exactly one word, hash is zero or more, and a star inside a word is not a wildcard."
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
