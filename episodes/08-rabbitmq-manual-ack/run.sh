#!/usr/bin/env bash
# One command per step of the lesson. Run it from anywhere; it always works inside this folder.
#
#   ./run.sh up              start RabbitMQ in Docker
#   ./run.sh experiment1     automatic acknowledgement: the worker dies while sending, and the message is gone
#   ./run.sh experiment2     manual acknowledgement: the same crash, and the message comes back
#   ./run.sh experiment3     manual acknowledgement: a crash after the email was sent, and two emails for one order
#   ./run.sh counters        print ready, unacknowledged and consumers of the queue
#   ./run.sh watch           print the counters twice a second (Ctrl+C to stop)
#   ./run.sh test            run the unit tests (no broker needed)
#   ./run.sh app <args>      run the program with your own arguments, e.g.  ./run.sh app work --mode manual --crash after-send
#   ./run.sh reset           delete the queue
#   ./run.sh down            stop RabbitMQ and delete its data
#
# Needs: Java 21, Maven, Docker. Every experiment starts from nothing, so you can repeat it.
# RabbitMQ of another episode uses the same ports: run "./run.sh down" in that episode's folder first.
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

QUEUE="notifications.email"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
app() { mvn -q exec:java -Dexec.args="$*"; }
build() { mvn -q -DskipTests compile; }
ctl() { docker compose exec -T rabbitmq rabbitmqctl -q --no-table-headers "$@" 2>/dev/null; }

# "queue  ready  unacked  consumers"; a queue that does not exist yet shows dashes
counters() {
  printf '%-22s %6s %8s %10s\n' queue ready unacked consumers
  ctl list_queues name messages_ready messages_unacknowledged consumers |
    awk -v q="$QUEUE" '$1 == q { found = 1; printf "%-22s %6s %8s %10s\n", $1, $2, $3, $4 } END { if (!found) printf "%-22s %6s %8s %10s\n", q, "-", "-", "-" }'
}

# Polls the broker until the queue shows exactly these numbers. usage: wait_for <ready> <unacked> <consumers|any> <seconds>
wait_for() {
  local ready="$1" unacked="$2" consumers="$3" seconds="$4" r u c i
  for ((i = 0; i < seconds * 4; i++)); do
    read -r r u c <<< "$(ctl list_queues name messages_ready messages_unacknowledged consumers | awk -v q="$QUEUE" '$1 == q { print $2, $3, $4 }')"
    if [[ "${r:-}" == "$ready" && "${u:-}" == "$unacked" && ( "$consumers" == any || "${c:-}" == "$consumers" ) ]]; then return 0; fi
    sleep 0.25
  done
  echo "(the queue did not reach ready $ready, unacked $unacked, consumers $consumers in $seconds s)" >&2
  return 1
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
    say "1/4  Automatic mode. The queue is empty and no worker is connected"
    counters
    say "2/4  The order service publishes evt_0001 for order ord_1001"
    app publish --count 1
    counters
    say "3/4  A worker in AUTOMATIC mode takes it and starts sending the email. Look at the counters while it works"
    app work --mode auto --name email-worker-1 --crash while-sending --send-ms 8000 --limit 1 &
    worker=$!
    wait_for 0 0 1 30
    counters
    say "4/4  The worker dies halfway through the email: the JVM halts, with no clean shutdown"
    wait "$worker" || true
    wait_for 0 0 0 30
    counters
    echo
    echo "In automatic mode the broker forgot the message when it sent it. Nothing is left to redeliver, and the email was never sent."
    ;;
  experiment2)
    fresh
    say "1/5  Manual mode. The order service publishes evt_0001 for order ord_1001"
    app publish --count 1
    counters
    say "2/5  A worker in MANUAL mode takes it and starts sending. The message is not ready any more, and it is not gone: it is unacknowledged"
    app work --mode manual --name email-worker-1 --crash while-sending --send-ms 8000 --limit 1 &
    worker=$!
    wait_for 0 1 1 30
    counters
    say "3/5  The worker dies halfway through the email. The broker sees the connection close and puts the message back"
    wait "$worker" || true
    wait_for 1 0 0 30
    counters
    say "4/5  A second worker takes the same message again: redelivered=true. It sends the email and acknowledges"
    app work --mode manual --name email-worker-2 --send-ms 3000 --limit 1
    say "5/5  The acknowledgement told the broker the job is done, so the queue is empty"
    wait_for 0 0 any 30
    counters
    echo
    echo "The same crash lost nothing this time: an unacknowledged message goes back to the queue when the worker's connection closes."
    ;;
  experiment3)
    fresh
    sends="$(mktemp)"
    say "1/4  Manual mode. The order service publishes evt_0001 for order ord_1001"
    app publish --count 1
    counters
    say "2/4  Worker one sends the email, logs 'email sent', and dies BEFORE the acknowledgement"
    app work --mode manual --name email-worker-1 --crash after-send --send-ms 3000 --limit 1 2>&1 | tee -a "$sends" || true
    wait_for 1 0 0 30
    counters
    say "3/4  The broker cannot know the email went out: it puts the message back. Worker two receives it again, redelivered=true"
    app work --mode manual --name email-worker-2 --send-ms 3000 --limit 1 2>&1 | tee -a "$sends"
    say "4/4  The queue is empty, and the log says how many emails went out for order ord_1001"
    wait_for 0 0 any 30
    counters
    echo
    echo "Emails sent for ord_1001: $(grep -c 'email sent for ord_1001' "$sends")"
    rm -f "$sends"
    echo "An acknowledgement is the worker's claim that the job is done. Without it the broker plays safe and delivers again, so the email goes out twice."
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
    sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
  *)
    echo "Unknown command: $1 (try ./run.sh help)" >&2
    exit 2
    ;;
esac
