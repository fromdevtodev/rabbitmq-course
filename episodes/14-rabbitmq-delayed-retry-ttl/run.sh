#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh hammering       the mail server is down for 25 s and the worker requeues: it is called again and again, and the next order waits
#   ./run.sh sleeping        the worker sleeps before retrying: the next order still waits, and a crash erases the delay
#   ./run.sh build           add the retry queue (message TTL) and the policy that sends rejected messages to it
#   ./run.sh retry-queue     the same orders with the retry queue: the next order is not held up, and the failed one comes back after the delay
#   ./run.sh counters        print ready, unacknowledged and consumers of the work queue, and the messages in the retry queue
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app work --on-failure retry-queue
#   ./run.sh reset           delete the queues, the exchanges and the policy
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from nothing, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUEUE=notifications.email
RETRY_QUEUE=notifications.email.retry
DELAY_MS="${RETRY_DELAY_MS:-10000}"   # the retry delay: the message TTL of the retry queue

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
compile() { mvn -q -DskipTests compile; }
ctl() { docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers "$@" 2>/dev/null; }

# region readme:policy
# Every message that the work queue dead-letters goes to the exchange orders.retry. A policy, not a queue argument, so it can be
# changed later without deleting the queue.
set_policy() {
  ctl set_policy retry-dlx '^notifications\.email$' '{"dead-letter-exchange":"orders.retry"}' --apply-to queues
}
# endregion

# "ready  unacked  consumers  in-retry": the work queue and the retry queue; a queue that does not exist yet shows dashes
counters() {
  printf '%-8s %8s %10s %9s\n' ready unacked consumers retry
  ctl list_queues name messages_ready messages_unacknowledged consumers messages |
    awk -v q="$QUEUE" -v r="$RETRY_QUEUE" '$1 == q { w = sprintf("%-8s %8s %10s", $2, $3, $4); f = 1 } $1 == r { retry = $5 }
      END { if (!f) w = sprintf("%-8s %8s %10s", "-", "-", "-"); printf "%s %9s\n", w, (retry == "" ? "-" : retry) }'
}

# the number of calls to the mail server about evt_0002, from the worker's log
calls_about() { grep -c "received evt_0002 " "$1" || true; }

fresh_plain() { compile; app reset; ctl clear_policy retry-dlx >/dev/null || true; app setup; }
build_retry() { compile; app build-retry --delay-ms "$DELAY_MS"; set_policy; }
fresh_retry() { compile; app reset; ctl clear_policy retry-dlx >/dev/null || true; build_retry; }

# Starts a worker in the background; its output goes to the file in $1. Sets WORKER_PID.
start_worker() {
  local log="$1"; shift
  mvn -q exec:java -Dexec.args="work $*" > "$log" 2>&1 &
  WORKER_PID=$!
  trap 'kill -9 "$WORKER_PID" 2>/dev/null || true' EXIT
}
wait_empty() {  # until the work queue and the retry queue are both empty, up to $1 seconds
  for ((i = 0; i < $1 * 2; i++)); do
    local c; c="$(counters | tail -1)"
    [[ "$(awk '{r = ($4 == "-") ? 0 : $4; print $1, $2, r}' <<< "$c")" == "0 0 0" ]] && return 0
    sleep 0.5
  done
  return 1
}

case "${1:-help}" in
  up)
    docker compose up -d --wait
    compile
    echo "RabbitMQ is up. Management page: http://localhost:15672 (guest / guest, local use only)."
    ;;
  hammering)
    fresh_plain
    say "1/4  Three orders. The mail server of evt_0002 is down for 25 seconds after the first call"
    app publish --first 1 --count 3
    counters
    say "2/4  The worker nacks with requeue=true; it is called again at once, again and again"
    LOG="$(mktemp)"
    start_worker "$LOG" --on-failure requeue --work-ms 300 --down-seconds 25
    sleep 4
    counters
    say "3/4  After the 25 seconds the mail server works: evt_0002 is sent, and only then evt_0003"
    wait_empty 30 || { echo "The queue did not empty in 30 s"; exit 1; }
    sleep 1; kill "$WORKER_PID" 2>/dev/null || true
    cat "$LOG" | grep -E "evt_0003|sent the email" || true
    say "4/4  How many times the mail server was called about evt_0002"
    echo "calls about evt_0002: $(calls_about "$LOG")   (count the 'received evt_0002' lines in the log yourself)"
    ;;
  sleeping)
    fresh_plain
    say "1/4  Three orders; the worker sleeps 10 s after a failure, holding the message"
    app publish --first 1 --count 3
    LOG="$(mktemp)"
    start_worker "$LOG" --on-failure sleep --work-ms 300 --sleep-ms 10000 --down-seconds 25
    sleep 3
    say "2/4  While it sleeps: evt_0002 is held (unacknowledged), and evt_0003 waits behind it (ready)"
    counters
    say "3/4  The worker crashes (kill -9) while it sleeps"
    kill -9 "$WORKER_PID"; sleep 2
    say "4/4  RabbitMQ saw the connection close: both orders are ready again, at once. The delay died with the worker"
    counters
    ;;
  build)
    build_retry
    say "The retry queue and the policy are in place"
    ctl list_policies
    counters
    ;;
  retry-queue)
    fresh_retry
    say "1/4  Three orders; the retry queue exists and the policy is set"
    app publish --first 1 --count 3
    LOG="$(mktemp)"
    start_worker "$LOG" --on-failure retry-queue --work-ms 300 --down-seconds 25
    sleep 3
    say "2/4  evt_0002 waits in the retry queue (retry 1) and evt_0003 has already been sent"
    counters
    grep -E "evt_0003|sent the email" "$LOG" || true
    say "3/4  After the delay evt_0002 is back, fails again while the server is still down, and goes round once more"
    wait_empty 40 || { echo "The queues did not empty in 40 s"; exit 1; }
    sleep 1; kill "$WORKER_PID" 2>/dev/null || true
    grep -E "evt_0002" "$LOG"
    say "4/4  The mail server was called $(calls_about "$LOG") times about evt_0002, one per delay"
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
    compile
    app "$@"
    ;;
  reset)
    compile
    app reset
    ctl clear_policy retry-dlx >/dev/null || true
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
