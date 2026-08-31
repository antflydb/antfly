#!/usr/bin/env bash
# Wrapper for watchdog-class Metal experiments (concurrent planned dispatch,
# barrier-affecting changes, unretained command buffers). METAL.md records a
# delayed (~90 s AFTER a passing run) SoC watchdog reset from this experiment
# class, and reboots that lost the failing test's identity. This wrapper makes
# any reset attributable and enforces the soak.
#
# Usage:
#   scripts/perf_watchdog_experiment.sh <experiment-id> <command...>
# Example:
#   TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH=1 \
#   scripts/perf_watchdog_experiment.sh concurrent-phaseA \
#     ./zig-out/bin/antfly-inference generate <model> "prompt" --backend metal ...
#
# Rules (GEMMA4_PERF_PLAN.md M0.5):
#   - Dedicated M4 Pro / CI box ONLY. Never a fanless machine.
#   - One watchdog-class experiment per boot.
#   - The post-pass soak is mandatory; a pass followed by a reset within the
#     soak window is a FAIL for the experiment.
set -u

SOAK_SECONDS="${WATCHDOG_SOAK_SECONDS:-300}"
INTENT_DIR="${WATCHDOG_INTENT_DIR:-$HOME/.antfly/perf-watchdog}"

EXPERIMENT_ID="${1:?experiment id required}"
shift
[ "$#" -ge 1 ] || { echo "command required" >&2; exit 2; }
case "$EXPERIMENT_ID" in
  ""|*[!A-Za-z0-9._-]*)
    echo "watchdog-experiment: experiment id must contain only letters, digits, '.', '_', or '-'" >&2
    exit 2
    ;;
esac
case "$SOAK_SECONDS" in
  ""|*[!0-9]*)
    echo "watchdog-experiment: WATCHDOG_SOAK_SECONDS must be an integer of at least 300" >&2
    exit 2
    ;;
esac
if [ "$SOAK_SECONDS" -lt 300 ]; then
  echo "watchdog-experiment: WATCHDOG_SOAK_SECONDS must be at least 300" >&2
  exit 2
fi
if ! mkdir -p "$INTENT_DIR"; then
  echo "watchdog-experiment: cannot create intent directory: $INTENT_DIR" >&2
  exit 1
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
INTENT_FILE="$INTENT_DIR/${STAMP}-${EXPERIMENT_ID}.intent"
GIT_SHA="$(git rev-parse HEAD 2>/dev/null || echo unknown)"

# Pre-commit the intent record and fsync it BEFORE the first dispatch so a
# hard reset cannot lose the experiment's identity.
if ! {
  echo "experiment_id=${EXPERIMENT_ID}"
  echo "started_at=${STAMP}"
  echo "git_sha=${GIT_SHA}"
  echo "machine=$(sysctl -n machdep.cpu.brand_string 2>/dev/null) $(sysctl -n hw.model 2>/dev/null)"
  echo "os=$(sw_vers -productVersion 2>/dev/null) $(sw_vers -buildVersion 2>/dev/null)"
  printf "command="
  printf " %q" "$@"
  printf "\n"
  env | grep -E '^(TERMITE_|ANTFLY_)' | sort
} > "$INTENT_FILE"; then
  echo "watchdog-experiment: cannot write intent record: $INTENT_FILE" >&2
  exit 1
fi
# fsync via a sync of the file's data
if ! /bin/sync; then
  echo "watchdog-experiment: cannot sync intent record: $INTENT_FILE" >&2
  exit 1
fi

echo "watchdog-experiment: intent recorded at $INTENT_FILE"
echo "watchdog-experiment: running: $*"
"$@"
RC=$?
echo "watchdog-experiment: command exited rc=$RC; soaking ${SOAK_SECONDS}s (reset window)"
if ! sleep "$SOAK_SECONDS"; then
  {
    echo "finished_at=$(date -u +%Y%m%dT%H%M%SZ)"
    echo "rc=${RC}"
    echo "soak_seconds=${SOAK_SECONDS}"
    echo "result=fail-soak-incomplete"
  } >> "$INTENT_FILE"
  echo "watchdog-experiment: soak did not complete; experiment is a FAIL" >&2
  exit 1
fi

echo "watchdog-experiment: soak complete; sweeping diagnostics"
PANICS=$(ls -t /Library/Logs/DiagnosticReports/Retired/panic-base-*.panic 2>/dev/null | head -3)
if [ -n "$PANICS" ]; then
  echo "watchdog-experiment: WARNING recent panic reports present:"
  echo "$PANICS"
fi
log show --last "$((SOAK_SECONDS / 60 + 2))m" --predicate 'eventMessage CONTAINS[c] "GPU" AND (eventMessage CONTAINS[c] "restart" OR eventMessage CONTAINS[c] "hang" OR eventMessage CONTAINS[c] "watchdog")' 2>/dev/null | tail -20

if ! {
  echo "finished_at=$(date -u +%Y%m%dT%H%M%SZ)"
  echo "rc=${RC}"
  echo "soak_seconds=${SOAK_SECONDS}"
  echo "result=$([ "$RC" -eq 0 ] && echo pass-pending-review || echo fail)"
} >> "$INTENT_FILE"; then
  echo "watchdog-experiment: cannot finalize intent record: $INTENT_FILE" >&2
  exit 1
fi
echo "watchdog-experiment: done; record appended to $INTENT_FILE (ledger flag: watchdog_class=true)"
exit "$RC"
