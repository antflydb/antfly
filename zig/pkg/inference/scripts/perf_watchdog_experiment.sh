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
mkdir -p "$INTENT_DIR"

EXPERIMENT_ID="${1:?experiment id required}"
shift
[ "$#" -ge 1 ] || { echo "command required" >&2; exit 2; }

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
INTENT_FILE="$INTENT_DIR/${STAMP}-${EXPERIMENT_ID}.intent"
GIT_SHA="$(git rev-parse HEAD 2>/dev/null || echo unknown)"

# Pre-commit the intent record and fsync it BEFORE the first dispatch so a
# hard reset cannot lose the experiment's identity.
{
  echo "experiment_id=${EXPERIMENT_ID}"
  echo "started_at=${STAMP}"
  echo "git_sha=${GIT_SHA}"
  echo "machine=$(sysctl -n machdep.cpu.brand_string 2>/dev/null) $(sysctl -n hw.model 2>/dev/null)"
  echo "os=$(sw_vers -productVersion 2>/dev/null) $(sw_vers -buildVersion 2>/dev/null)"
  echo "command=$*"
  env | grep -E '^(TERMITE_|ANTFLY_)' | sort
} > "$INTENT_FILE"
# fsync via a sync of the file's data
/bin/sync

echo "watchdog-experiment: intent recorded at $INTENT_FILE"
echo "watchdog-experiment: running: $*"
"$@"
RC=$?
echo "watchdog-experiment: command exited rc=$RC; soaking ${SOAK_SECONDS}s (reset window)"
sleep "$SOAK_SECONDS"

echo "watchdog-experiment: soak complete; sweeping diagnostics"
PANICS=$(ls -t /Library/Logs/DiagnosticReports/Retired/panic-base-*.panic 2>/dev/null | head -3)
if [ -n "$PANICS" ]; then
  echo "watchdog-experiment: WARNING recent panic reports present:"
  echo "$PANICS"
fi
log show --last "$((SOAK_SECONDS / 60 + 2))m" --predicate 'eventMessage CONTAINS[c] "GPU" AND (eventMessage CONTAINS[c] "restart" OR eventMessage CONTAINS[c] "hang" OR eventMessage CONTAINS[c] "watchdog")' 2>/dev/null | tail -20

{
  echo "finished_at=$(date -u +%Y%m%dT%H%M%SZ)"
  echo "rc=${RC}"
  echo "soak_seconds=${SOAK_SECONDS}"
  echo "result=$([ "$RC" -eq 0 ] && echo pass-pending-review || echo fail)"
} >> "$INTENT_FILE"
echo "watchdog-experiment: done; record appended to $INTENT_FILE (ledger flag: watchdog_class=true)"
exit "$RC"
