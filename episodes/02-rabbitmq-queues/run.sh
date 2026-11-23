#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker and declare the queue
#   ./run.sh experiment      five messages wait, consumer A takes them in order, consumer B finds nothing
#   ./run.sh counters        print the queue's counters once
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app publish --count 20
#   ./run.sh reset           delete the queue
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from an empty queue, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUEUE="notifications.email"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }

# "queue  ready  consumers" for the one queue of this episode
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
  experiment)
    fresh
    pids=()
    trap 'for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done' EXIT
    say "1/4  The queue exists and is empty"
    counters
    say "2/4  The producer publishes five messages and goes away: nobody is consuming yet"
    app publish --count 5
    counters
    say "3/4  Consumer A takes them one at a time (2.5 s each): watch ready fall, and the order of the events"
    app consume --name consumer-a --delay-ms 2500 --limit 5 &
    a=$!
    pids+=("$a")
    while kill -0 "$a" 2>/dev/null; do counters | tail -n 1; sleep 2; done
    wait "$a" || true
    counters
    say "4/4  Consumer B starts later: it connects and waits, there is nothing to receive"
    app consume --name consumer-b &
    pids+=("$!")
    for _ in $(seq 1 30); do [[ "$(consumers)" == "1" ]] && break; sleep 1; done
    sleep 2
    counters
    echo
    echo "The five messages are gone. A queue does not keep what a consumer has taken."
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
    sed -n '2,14p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "Unknown command: $1 (try ./run.sh help)" >&2
    exit 2
    ;;
esac
