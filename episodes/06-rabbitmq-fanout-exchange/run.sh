#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh experiment1     one publish, three queues, three different binding keys: every queue gets a copy
#   ./run.sh experiment2     one queue shared by two consumers, then one queue for each of two instances
#   ./run.sh experiment3     a fanout exchange with no queue bound: the message is discarded
#   ./run.sh counters        print how many messages each named queue holds
#   ./run.sh bindings        print the bindings of the exchange price-updates
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app publish --key toys --first 9
#   ./run.sh reset           delete the exchange and the four named queues
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from nothing, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

BROADCAST_QUEUES=(cache.instance-1 cache.instance-2 cache.instance-3)
NAMED_QUEUES=("${BROADCAST_QUEUES[@]}" cache.shared)
CONSUMER_LOG=""
CONSUMER_PIDS=()

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }
ctl() { docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers "$@" 2>/dev/null; }

# "queue  ready" for the queues given as arguments; a queue that does not exist yet shows a dash
counters_of() {
  printf '%-18s %s\n' queue ready
  local q
  for q in "$@"; do
    printf '%-18s %s\n' "$q" "$(ctl list_queues name messages_ready | awk -v q="$q" '$1 == q { found = 1; print $2 } END { if (!found) print "-" }')"
  done
}

# The four named queues of the project; only the ones that exist are shown.
counters() {
  printf '%-18s %s\n' queue ready
  ctl list_queues name messages_ready | awk -v want="${NAMED_QUEUES[*]}" \
    'BEGIN { n = split(want, w, " "); for (i = 1; i <= n; i++) mine[w[i]] = 1 } $1 in mine { printf "%-18s %s\n", $1, $2 }' | sort
}

# The bindings of the exchange price-updates, sorted by queue. Rows with another source name belong to other exchanges
# (the default exchange, for one). The key column is empty for the binding with an empty key.
bindings() {
  printf '%-14s %-18s %s\n' exchange queue key
  ctl list_bindings source_name destination_name routing_key | awk -F'\t' '$1 == "price-updates" { printf "%-14s %-18s %s\n", $1, $2, "[" $3 "]" }' | sort -k2,2
}

# Names of the queues that the broker named itself (exclusive queues of the instances).
generated_queues() { ctl list_queues name | grep -c '^amq\.gen-' || true; }

stop_consumers() {
  local pid
  for pid in "${CONSUMER_PIDS[@]:-}"; do
    [[ -z "$pid" ]] || { kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; }
  done
  CONSUMER_PIDS=()
}
trap stop_consumers EXIT

# Starts a consumer or an instance in the background; its lines go to $CONSUMER_LOG and are printed when it is ready.
start_receiver() {
  local name="$1"; shift
  mvn -q exec:java -Dexec.args="$*" >> "$CONSUMER_LOG" 2>&1 &
  CONSUMER_PIDS+=("$!")
  for _ in {1..80}; do
    grep -q "\[$name\] ready" "$CONSUMER_LOG" && return 0
    sleep 0.25
  done
  echo "ERROR: $name did not become ready. See $CONSUMER_LOG" >&2
  return 1
}

received() { grep -c "\[$1\] received" "$CONSUMER_LOG" || true; }

wait_received() {   # wait_received <total> <names...>
  local total="$1" sum n; shift
  for _ in {1..80}; do
    sum=0
    for n in "$@"; do sum=$((sum + $(received "$n"))); done
    (( sum == total )) && return 0
    sleep 0.25
  done
  echo "ERROR: expected $total receptions, saw $sum. See $CONSUMER_LOG" >&2
  return 1
}

fresh() { build; app reset; app "$1"; }

case "${1:-help}" in
  up)
    docker compose up -d --wait
    build
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  experiment1)
    fresh setup
    say "1/4  The exchange price-updates is a fanout exchange with three queues, bound with '', books and electronics; all queues are empty"
    bindings
    counters_of "${BROADCAST_QUEUES[@]}"
    say "2/4  evt_0001 has routing key toys: no binding key says toys, and every queue gets a copy"
    app publish --key toys --first 1
    counters_of "${BROADCAST_QUEUES[@]}"
    say "3/4  evt_0002 has routing key books: only one binding key says books, and every queue gets a copy"
    app publish --key books --first 2
    counters_of "${BROADCAST_QUEUES[@]}"
    say "4/4  evt_0003 has routing key electronics: the same again"
    app publish --key electronics --first 3
    counters_of "${BROADCAST_QUEUES[@]}"
    echo
    echo "A fanout exchange ignores the routing key and copies every message to every queue bound to it."
    ;;
  experiment2)
    CONSUMER_LOG="$(mktemp)"
    fresh setup-shared
    say "1/6  Design one: one queue, cache.shared, and two consumers that share it"
    start_receiver cache-a consume --name cache-a
    start_receiver cache-b consume --name cache-b
    say "2/6  Four price changes are published"
    app publish --key toys --count 4 --first 1
    wait_received 4 cache-a cache-b
    sleep 1
    say "3/6  Each of the four messages reached exactly one consumer"
    grep received "$CONSUMER_LOG" | sort
    for n in 1 2 3 4; do
      [[ "$(grep -c "received evt_000$n" "$CONSUMER_LOG")" == 1 ]] || { echo "ERROR: evt_000$n was not received exactly once" >&2; exit 1; }
    done
    echo "cache-a: $(received cache-a)   cache-b: $(received cache-b)   total: 4 (the split between the two is not the point)"
    stop_consumers
    : > "$CONSUMER_LOG"
    app reset      # clear the broker: cache.shared must not stay bound to the exchange
    say "4/6  Design two: two instances, each one with an exclusive queue named by the broker"
    start_receiver instance-1 instance --name instance-1
    start_receiver instance-2 instance --name instance-2
    ctl list_queues name | sort
    say "5/6  Four price changes are published: every message is received twice, once by each instance"
    app publish --key toys --count 4 --first 5
    wait_received 8 instance-1 instance-2
    sleep 1
    grep received "$CONSUMER_LOG" | sort
    for n in 5 6 7 8; do
      [[ "$(grep -c "received evt_000$n" "$CONSUMER_LOG")" == 2 ]] || { echo "ERROR: evt_000$n was not received twice" >&2; exit 1; }
    done
    say "6/6  Stop the instances: their queues are deleted with their connections"
    stop_consumers
    for _ in {1..20}; do [[ "$(generated_queues)" == 0 ]] && break; sleep 0.5; done
    ctl list_queues name | sort
    [[ "$(generated_queues)" == 0 ]] || { echo "ERROR: a queue named by the broker is still there" >&2; exit 1; }
    echo
    echo "One queue: the consumers share its messages. One queue each: every instance gets its own copy."
    ;;
  experiment3)
    fresh setup-empty
    say "1/2  The exchange price-updates has no queue bound"
    bindings
    say "2/2  A price change is published: the publish succeeds, and the message is discarded"
    app publish --key toys --first 1
    counters
    echo
    echo "No queue is bound, so there is nobody to copy to; no error is reported to the publisher."
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
