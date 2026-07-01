#!/usr/bin/env bash
# Static hygiene audit for the TLA+ model suite (specs/tla).
#
# Hard failures (exit 1):
#   - *Bad*.cfg files not wired into the Makefile's tla-check-negative graph
#   - leftover _TTrace_ artifacts
#   - Buggy* constants declared in a spec with no expected-failure config
#     that enables them
# Reported (non-fatal, tracked migration debt):
#   - *Bad*.cfg files that pin the broad Safety conjunction instead of a
#     named semantic invariant
#
# Runtime complement: the tla_check_expected_failure Make macro requires an
# actual "Invariant ... is violated" / temporal violation in TLC output, so
# mutants that fail for non-invariant reasons (spec/config errors) fail the
# negative harness rather than passing silently.

set -uo pipefail

SPEC_DIR="${SPEC_DIR:-specs/tla}"
MAKEFILE="${MAKEFILE_PATH:-Makefile}"
fail=0

echo "== tla-audit: ${SPEC_DIR}"

# --- 1. Safety-pinned negative configs (migration debt, non-fatal) ---------
safety_pinned=()
for cfg in "${SPEC_DIR}"/*Bad*.cfg; do
    [ -e "$cfg" ] || continue
    if grep -qE '^INVARIANT Safety$' "$cfg" || \
       (grep -qE '^INVARIANTS' "$cfg" && grep -qE '^    Safety$' "$cfg"); then
        safety_pinned+=("$(basename "$cfg")")
    fi
done
if [ "${#safety_pinned[@]}" -gt 0 ]; then
    echo "-- ${#safety_pinned[@]} negative config(s) still pin the Safety conjunction (migration pending):"
    printf '   %s\n' "${safety_pinned[@]}"
else
    echo "-- all negative configs pin named invariants"
fi

# --- 2. Orphan negative configs not wired into the Makefile ----------------
orphans=0
for cfg in "${SPEC_DIR}"/*Bad*.cfg; do
    [ -e "$cfg" ] || continue
    name="$(basename "$cfg" .cfg)"
    if ! grep -q "$name" "$MAKEFILE"; then
        echo "FAIL: orphan negative config not wired into Makefile: $name"
        orphans=$((orphans + 1))
        fail=1
    fi
done
[ "$orphans" -eq 0 ] && echo "-- all negative configs are wired into the Makefile"

# --- 3. TTrace artifacts ----------------------------------------------------
ttrace="$(find "${SPEC_DIR}" -maxdepth 1 \( -name '*_TTrace_*.tla' -o -name '*_TTrace_*.bin' \) | sort)"
if [ -n "$ttrace" ]; then
    echo "FAIL: leftover TLC trace artifacts (run 'make tla-clean'):"
    echo "$ttrace" | sed 's/^/   /'
    fail=1
else
    echo "-- no _TTrace_ artifacts"
fi

# --- 4. Buggy* constants without an enabling expected-failure config -------
# Only identifiers declared in a CONSTANTS block count; Buggy*-named actions
# are guarded by those constants and would be false positives.
unexercised=0
for spec in "${SPEC_DIR}"/Antfly*.tla; do
    [ -e "$spec" ] || continue
    case "$spec" in *_TTrace_*|*Trace*) continue ;; esac
    consts="$(awk '/^CONSTANTS?([[:space:]]|$)/{inblock=1} inblock{print; if ($0 !~ /,[[:space:]]*$/ && $0 !~ /^CONSTANTS?[[:space:]]*$/) inblock=0}' "$spec" \
        | grep -oE 'Buggy[A-Za-z0-9]+' | sort -u)"
    for const in $consts; do
        if ! grep -lE "${const}[[:space:]]*=[[:space:]]*TRUE" "${SPEC_DIR}"/*.cfg >/dev/null 2>&1; then
            echo "FAIL: $(basename "$spec"): constant ${const} has no config enabling it"
            unexercised=$((unexercised + 1))
            fail=1
        fi
    done
done
[ "$unexercised" -eq 0 ] && echo "-- every Buggy* constant has an enabling expected-failure config"

if [ "$fail" -ne 0 ]; then
    echo "== tla-audit: FAILED"
    exit 1
fi
echo "== tla-audit: OK (${#safety_pinned[@]} Safety-pinned config(s) pending migration)"
