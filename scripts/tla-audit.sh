#!/usr/bin/env bash
# Static hygiene audit for the TLA+ model suite (specs/tla).
#
# Layout: each model <Model>.tla has a sibling <Model>.cfgs holding all of
# its checks as named sections in verbatim TLC config syntax (extracted at
# build time by scripts/tla-cfg.sh). Vendored/legacy specs (etcdraft family,
# occ-2pc) keep plain .cfg files.
#
# Hard failures (exit 1):
#   - Bad* check sections not wired into the Makefile
#   - leftover _TTrace_ artifacts
#   - Buggy* constants declared in a spec with no check section enabling them
# Reported (non-fatal, tracked migration debt):
#   - Bad* sections that pin the broad Safety conjunction instead of a named
#     semantic invariant
#
# Runtime complement: the tla_check_expected_failure Make macro requires an
# actual "Invariant ... is violated" / temporal violation in TLC output, so
# mutants that fail for non-invariant reasons (spec/config errors) fail the
# negative harness rather than passing silently.

set -uo pipefail

SPEC_ROOT="${SPEC_ROOT:-specs/tla}"
MAKEFILE="${MAKEFILE_PATH:-Makefile}"
fail=0

echo "== tla-audit: ${SPEC_ROOT}"

# Enumerate all (file, section) pairs across .cfgs files.
list_sections() { # $1 = .cfgs path -> prints section names
    awk '/^==== /{print $2}' "$1"
}
section_body() { # $1 = .cfgs path, $2 = section
    awk -v want="$2" '/^==== /{insec=($2==want); next} insec{print}' "$1"
}

# --- 1. Safety-pinned negative sections (migration debt, non-fatal) --------
safety_pinned=()
while IFS= read -r cfgs; do
    model="$(basename "$cfgs" .cfgs)"
    while IFS= read -r sec; do
        case "$sec" in Bad*) ;; *) continue ;; esac
        body="$(section_body "$cfgs" "$sec")"
        if echo "$body" | grep -qE '^INVARIANT Safety$' || \
           { echo "$body" | grep -qE '^INVARIANTS' && echo "$body" | grep -qE '^    Safety$'; }; then
            safety_pinned+=("${model}:${sec}")
        fi
    done < <(list_sections "$cfgs")
done < <(find "${SPEC_ROOT}" -name '*.cfgs' -not -path '*/.generated/*' | sort)
if [ "${#safety_pinned[@]}" -gt 0 ]; then
    echo "-- ${#safety_pinned[@]} negative check(s) still pin the Safety conjunction (migration pending):"
    printf '   %s\n' "${safety_pinned[@]}"
else
    echo "-- all negative checks pin named invariants"
fi

# --- 2. Orphan negative sections not wired into the Makefile ---------------
orphans=0
while IFS= read -r cfgs; do
    model="$(basename "$cfgs" .cfgs)"
    while IFS= read -r sec; do
        case "$sec" in Bad*) ;; *) continue ;; esac
        if ! grep -q "${model}${sec}" "$MAKEFILE"; then
            echo "FAIL: negative check not wired into Makefile: ${model}:${sec}"
            orphans=$((orphans + 1))
            fail=1
        fi
    done < <(list_sections "$cfgs")
done < <(find "${SPEC_ROOT}" -name '*.cfgs' -not -path '*/.generated/*' | sort)
[ "$orphans" -eq 0 ] && echo "-- all negative checks are wired into the Makefile"

# --- 3. TTrace artifacts ----------------------------------------------------
ttrace="$(find "${SPEC_ROOT}" \( -name '*_TTrace_*.tla' -o -name '*_TTrace_*.bin' \) -not -path '*/.generated/*' | sort)"
if [ -n "$ttrace" ]; then
    echo "FAIL: leftover TLC trace artifacts (run 'make tla-clean'):"
    echo "$ttrace" | sed 's/^/   /'
    fail=1
else
    echo "-- no _TTrace_ artifacts"
fi

# --- 4. Buggy* constants without an enabling check section -----------------
# Only identifiers declared in a CONSTANTS block count; Buggy*-named actions
# are guarded by those constants and would be false positives.
unexercised=0
while IFS= read -r spec; do
    case "$spec" in *_TTrace_*|*Trace*) continue ;; esac
    dir="$(dirname "$spec")"
    consts="$(awk '/^CONSTANTS?([[:space:]]|$)/{inblock=1} inblock{print; if ($0 !~ /,[[:space:]]*$/ && $0 !~ /^CONSTANTS?[[:space:]]*$/) inblock=0}' "$spec" \
        | grep -oE 'Buggy[A-Za-z0-9]+' | sort -u)"
    for const in $consts; do
        if ! grep -qE "${const}[[:space:]]*=[[:space:]]*TRUE" "$dir"/*.cfgs "$dir"/*.cfg 2>/dev/null; then
            echo "FAIL: $(basename "$spec"): constant ${const} has no check enabling it"
            unexercised=$((unexercised + 1))
            fail=1
        fi
    done
done < <(find "${SPEC_ROOT}" -name 'Antfly*.tla' -not -path '*/.generated/*' | sort)
[ "$unexercised" -eq 0 ] && echo "-- every Buggy* constant has an enabling check"

if [ "$fail" -ne 0 ]; then
    echo "== tla-audit: FAILED"
    exit 1
fi
echo "== tla-audit: OK (${#safety_pinned[@]} Safety-pinned check(s) pending migration)"
