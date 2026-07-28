#!/usr/bin/env bash
# The TLA+ check runner. Everything TLA-related that is not TLC itself lives
# here; the Makefile only provides names (tla-check, tla-trace, tla-clean).
#
#   tla-check.sh gate               audit + smoke + core + fast + all mutants
#   tla-check.sh tier <name>        run every positive check in a tier
#                                   (core | fast | heavy | manual)
#   tla-check.sh run <check-id>     run one check by id
#   tla-check.sh negative           run every Bad* mutant and the negative
#                                   trace fixtures; each must fail on a real
#                                   invariant/property violation
#   tla-check.sh smoke              SANY-parse every spec, no model checking
#   tla-check.sh audit              static hygiene audit
#   tla-check.sh list               list all checks with tiers
#   tla-check.sh trace <family>     validate NDJSON traces (TRACE_FILES=...)
#                                   family: raft | txn | txn-session | ha |
#                                   split-bridge | doc-identity-range-repair |
#                                   placement-readiness | index-lifecycle |
#                                   derived-replay | enrichment-lease
#
# Layout: each model <Model>.tla has a sibling <Model>.cfgs holding all of
# its checks as named sections in verbatim TLC config syntax:
#
#   ==== positive tier=fast
#   SPECIFICATION ...
#   ==== BadSomething
#   ...
#
# Check ids are <Model> (positive), <Model><BadX> (mutants), and
# <Model>-<variant> (heavy/safety variants). Tier membership and MC-wrapper
# spec overrides (spec=...) are annotations on the section header. Sections
# are extracted to specs/tla/.generated/ per run. Vendored specs (etcdraft
# family, occ-2pc) keep plain checked-in .cfg files.

set -uo pipefail

SPEC_ROOT="${SPEC_ROOT:-specs/tla}"
GEN_DIR="${SPEC_ROOT}/.generated"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${SCRIPT_DIR}/tla-tools.sh"
set +e +o errexit   # tla-tools.sh sets -e; this script tests statuses itself
set -uo pipefail

# --- section extraction (config resolution) ----------------------------------

resolve_paths() { # $1 = check-id, $2 = module, $3 = spec-module -> TLC_CFG/TLC_SPEC
    local check="$1" module="$2" spec_module="$3"
    local owner dir cfgs section
    owner="$(find "${SPEC_ROOT}" -name "${module}.tla" -not -path '*/.generated/*' | head -1)"
    [ -n "$owner" ] || { echo "tla-check: module ${module}.tla not found" >&2; return 1; }
    dir="$(dirname "$owner")"
    if [ "$spec_module" = "$module" ]; then
        TLC_SPEC="$owner"
    else
        TLC_SPEC="$(find "${SPEC_ROOT}" -name "${spec_module}.tla" -not -path '*/.generated/*' | head -1)"
        [ -n "$TLC_SPEC" ] || { echo "tla-check: spec module ${spec_module}.tla not found" >&2; return 1; }
    fi
    cfgs="${dir}/${module}.cfgs"
    if [ ! -f "$cfgs" ]; then
        TLC_CFG="${dir}/${check}.cfg"   # vendored/legacy layout
        [ -f "$TLC_CFG" ] || { echo "tla-check: neither ${cfgs} nor ${TLC_CFG} exists" >&2; return 1; }
        return 0
    fi
    section="${check#"${module}"}"; section="${section#-}"; [ -z "$section" ] && section="positive"
    mkdir -p "$GEN_DIR"
    TLC_CFG="${GEN_DIR}/${check}.cfg"
    awk -v want="$section" '/^==== /{insec=($2==want); next} insec{print}' "$cfgs" > "$TLC_CFG"
    [ -s "$TLC_CFG" ] || { echo "tla-check: section '${section}' not found in ${cfgs}" >&2; return 1; }
}

# --- check discovery ----------------------------------------------------------

enumerate() { # prints: check-id<TAB>model<TAB>section<TAB>tier<TAB>spec-override
    while IFS= read -r cfgs; do
        local model
        model="$(basename "$cfgs" .cfgs)"
        case "$model" in Trace*) continue ;; esac
        awk -v model="$model" '
            /^==== / {
                sec = $2; tier = "-"; spec = model
                for (i = 3; i <= NF; i++) {
                    if ($i ~ /^tier=/) { tier = substr($i, 6) }
                    if ($i ~ /^spec=/) { spec = substr($i, 6) }
                }
                id = model
                if (sec ~ /^Bad/) { id = model sec; tier = "negative" }
                else if (sec != "positive") { id = model "-" sec }
                printf "%s\t%s\t%s\t%s\t%s\n", id, model, sec, tier, spec
            }' "$cfgs"
    done < <(find "${SPEC_ROOT}" -name '*.cfgs' -not -path '*/.generated/*' | sort)
}

resolve_check() { # $1 = check-id -> CHECK_MODEL/CHECK_SECTION/CHECK_TIER/CHECK_SPEC
    local row
    row="$(enumerate | awk -F'\t' -v id="$1" '$1 == id')"
    [ -n "$row" ] || { echo "tla-check: unknown check id '$1' (see: tla-check.sh list)" >&2; return 1; }
    IFS=$'\t' read -r _ CHECK_MODEL CHECK_SECTION CHECK_TIER CHECK_SPEC <<<"$row"
}

# --- runners -------------------------------------------------------------------

run_positive() { # $1 = check-id
    resolve_check "$1" && resolve_paths "$1" "$CHECK_MODEL" "$CHECK_SPEC" || return 1
    echo "==> Model checking ${1}..."
    "$TLA_JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC \
        -config "$TLC_CFG" "$TLC_SPEC" -workers auto -deadlock
}

run_expect_fail() { # $1 = check-id
    resolve_check "$1" && resolve_paths "$1" "$CHECK_MODEL" "$CHECK_SPEC" || return 1
    echo "==> Model checking expected-failure ${1}..."
    local out status=0
    out="$("$TLA_JAVA" -XX:+UseParallelGC -cp "$TLA2TOOLS" tlc2.TLC \
        -config "$TLC_CFG" "$TLC_SPEC" -workers auto -deadlock 2>&1)" || status=$?
    echo "$out"
    if [ "$status" -eq 0 ]; then
        echo "ERROR: expected ${1} to fail, but TLC passed"; return 1
    elif echo "$out" | grep -qE "Invariant .* is violated|Temporal property .* (is |was )?violated"; then
        echo "OK: ${1} failed as expected"
    else
        echo "ERROR: ${1} failed, but not on an invariant/property violation (spec or config error?)"; return 1
    fi
}

run_tier() { # $1 = tier
    local ids
    ids="$(enumerate | awk -F'\t' -v t="$1" '$4 == t {print $1}')"
    [ -n "$ids" ] || { echo "tla-check: no checks in tier '$1'" >&2; return 1; }
    for id in $ids; do run_positive "$id" || return 1; done
}

# Negative trace fixtures: trace-model + fixture that must FAIL validation.
NEG_TRACE_FIXTURES=(
    "TraceAntflyTransactionSession ${SPEC_ROOT}/traces/negative/txn_session_bad_cleanup.ndjson"
    "TraceAntflySplitRefinementBridge ${SPEC_ROOT}/traces/negative/split_bridge_route_before_db_serving.ndjson"
    "TraceAntflyDocumentIdentityRangeRepair ${SPEC_ROOT}/traces/negative/doc_identity_restore_accept_mismatch.ndjson"
    "TraceAntflyDocumentIdentityRangeRepair ${SPEC_ROOT}/traces/negative/doc_identity_restore_early_clear.ndjson"
    "TraceAntflyPlacementReadiness ${SPEC_ROOT}/traces/negative/placement_readiness_unknown_latches_ambiguity.ndjson"
    "TraceAntflyIndexLifecycle ${SPEC_ROOT}/traces/negative/index_lifecycle_lost_second_wakeup.ndjson"
    "TraceAntflyDerivedReplay ${SPEC_ROOT}/traces/negative/derived_replay_advance_beyond_target.ndjson"
    "TraceAntflyEnrichmentLease ${SPEC_ROOT}/traces/negative/enrichment_stale_owner_publish.ndjson"
)

run_negative() {
    for id in $(enumerate | awk -F'\t' '$4 == "negative" {print $1}'); do
        run_expect_fail "$id" || return 1
    done
    local model fixture
    for entry in "${NEG_TRACE_FIXTURES[@]}"; do
        read -r model fixture <<<"$entry"
        resolve_paths "$model" "$model" "$model" || return 1
        echo "==> Validating expected-failure trace fixture ${fixture}..."
        if bash "${SCRIPT_DIR}/tla-validate-trace.sh" -S -p 1 \
            -s "$TLC_SPEC" -c "$TLC_CFG" "$fixture"; then
            echo "ERROR: expected ${fixture} to fail validation, but it passed"; return 1
        else
            echo "OK: ${fixture} failed as expected"
        fi
    done
}

run_smoke() {
    echo "==> Parsing checked-in TLA+ specs..."
    local spec
    while IFS= read -r spec; do
        case "$spec" in *_TTrace_*|*/occ-2pc.tla) continue ;; esac
        (cd "$(dirname "$spec")" && \
         "$TLA_JAVA" -cp "$TLA2TOOLS" tla2sany.SANY "$(basename "$spec")" >/dev/null) || return 1
    done < <(find "${SPEC_ROOT}" -name '*.tla' -not -path '*/.generated/*' | sort)
}

run_trace() { # $1 = family; TRACE_FILES env required
    local family="$1"
    : "${TRACE_FILES:?TRACE_FILES is required, e.g. TRACE_FILES=specs/tla/traces/ha_*.ndjson}"
    case "$family" in
    raft)
        local segdir segments
        segdir="$(mktemp -d)"; trap 'rm -rf "$segdir"' RETURN
        for f in $TRACE_FILES; do
            python3 "${SCRIPT_DIR}/tla-segment-raft-trace.py" "$f" "$segdir" || return 1
        done
        segments="$(find "$segdir" -name '*.ndjson' -size +0c | sort)"
        [ -n "$segments" ] || { echo "No non-empty trace segments found"; return 1; }
        bash "${SCRIPT_DIR}/tla-validate-trace.sh" -S \
            -s "${SPEC_ROOT}/Traceetcdraft.tla" -c "${SPEC_ROOT}/Traceetcdraft.cfg" $segments
        ;;
    txn)
        local validated=0 filtered segdir segments
        for f in $TRACE_FILES; do
            filtered="$(mktemp)"
            python3 "${SCRIPT_DIR}/tla-filter-txn-trace.py" < "$f" > "$filtered" || { rm -f "$filtered"; return 1; }
            if [ -s "$filtered" ]; then
                segdir="$(mktemp -d)"
                python3 "${SCRIPT_DIR}/tla-segment-txn-trace.py" "$filtered" "$segdir" || { rm -f "$filtered"; rm -rf "$segdir"; return 1; }
                segments="$(find "$segdir" -name '*.ndjson' -size +0c | sort)"
                if [ -n "$segments" ]; then
                    resolve_paths TraceAntflyTransaction TraceAntflyTransaction TraceAntflyTransaction || return 1
                    bash "${SCRIPT_DIR}/tla-validate-trace.sh" -S \
                        -s "$TLC_SPEC" -c "$TLC_CFG" $segments || { rm -f "$filtered"; rm -rf "$segdir"; return 1; }
                    validated=$((validated + 1))
                else
                    echo "SKIP $f (no non-empty transaction trace segments)"
                fi
                rm -rf "$segdir"
            else
                echo "SKIP $f (no spec-compatible transactions after filtering)"
            fi
            rm -f "$filtered"
        done
        [ "$validated" -gt 0 ] || { echo "No spec-compatible transactions found after filtering"; return 1; }
        ;;
    txn-session|ha|split-bridge|doc-identity-range-repair|placement-readiness|index-lifecycle|derived-replay|enrichment-lease)
        local model
        case "$family" in
        txn-session) model=TraceAntflyTransactionSession ;;
        ha) model=TraceAntflyHA ;;
        split-bridge) model=TraceAntflySplitRefinementBridge ;;
        doc-identity-range-repair) model=TraceAntflyDocumentIdentityRangeRepair ;;
        placement-readiness) model=TraceAntflyPlacementReadiness ;;
        index-lifecycle) model=TraceAntflyIndexLifecycle ;;
        derived-replay) model=TraceAntflyDerivedReplay ;;
        enrichment-lease) model=TraceAntflyEnrichmentLease ;;
        esac
        resolve_paths "$model" "$model" "$model" || return 1
        bash "${SCRIPT_DIR}/tla-validate-trace.sh" -S -p 1 \
            -s "$TLC_SPEC" -c "$TLC_CFG" $TRACE_FILES
        ;;
    *)
        echo "tla-check: unknown trace family '$family' (raft|txn|txn-session|ha|split-bridge|doc-identity-range-repair|placement-readiness|index-lifecycle|derived-replay|enrichment-lease)" >&2
        return 1
        ;;
    esac
}

# --- audit ---------------------------------------------------------------------

run_audit() {
    local fail=0
    echo "== tla-audit: ${SPEC_ROOT}"

    # 1. Safety-pinned mutants (migration debt, non-fatal)
    local safety_pinned=()
    local cfgs model sec body hdr
    while IFS= read -r cfgs; do
        model="$(basename "$cfgs" .cfgs)"
        while IFS= read -r sec; do
            case "$sec" in Bad*) ;; *) continue ;; esac
            body="$(awk -v want="$sec" '/^==== /{insec=($2==want); next} insec{print}' "$cfgs")"
            if echo "$body" | grep -qE '^INVARIANT Safety$' || \
               { echo "$body" | grep -qE '^INVARIANTS' && echo "$body" | grep -qE '^    Safety$'; }; then
                safety_pinned+=("${model}:${sec}")
            fi
        done < <(awk '/^==== /{print $2}' "$cfgs")
    done < <(find "${SPEC_ROOT}" -name '*.cfgs' -not -path '*/.generated/*' | sort)
    if [ "${#safety_pinned[@]}" -gt 0 ]; then
        echo "-- ${#safety_pinned[@]} mutant(s) still pin the Safety conjunction (migration pending):"
        printf '   %s\n' "${safety_pinned[@]}"
    else
        echo "-- all mutants pin named invariants"
    fi

    # 2. Non-mutant sections missing a tier annotation would never run.
    local untiered=0
    while IFS= read -r cfgs; do
        model="$(basename "$cfgs" .cfgs)"
        case "$model" in Trace*) continue ;; esac
        while IFS= read -r hdr; do
            sec="$(echo "$hdr" | awk '{print $2}')"
            case "$sec" in Bad*) continue ;; esac
            if ! echo "$hdr" | grep -q 'tier='; then
                echo "FAIL: section without tier annotation (never runs): ${model}:${sec}"
                untiered=$((untiered + 1)); fail=1
            fi
        done < <(grep '^==== ' "$cfgs")
    done < <(find "${SPEC_ROOT}" -name '*.cfgs' -not -path '*/.generated/*' | sort)
    [ "$untiered" -eq 0 ] && echo "-- every non-mutant section has a tier annotation"

    # 3. TTrace artifacts
    local ttrace
    ttrace="$(find "${SPEC_ROOT}" \( -name '*_TTrace_*.tla' -o -name '*_TTrace_*.bin' \) -not -path '*/.generated/*' | sort)"
    if [ -n "$ttrace" ]; then
        echo "FAIL: leftover TLC trace artifacts (run 'make tla-clean'):"
        echo "$ttrace" | sed 's/^/   /'; fail=1
    else
        echo "-- no _TTrace_ artifacts"
    fi

    # 4. Buggy* CONSTANTS never enabled by any check
    local unexercised=0 spec dir consts const
    while IFS= read -r spec; do
        case "$spec" in *_TTrace_*|*Trace*) continue ;; esac
        dir="$(dirname "$spec")"
        consts="$(awk '/^CONSTANTS?([[:space:]]|$)/{inblock=1} inblock{line=$0; sub(/\\\*.*$/, "", line); print; if (line !~ /,[[:space:]]*$/ && line !~ /^CONSTANTS?[[:space:]]*$/) inblock=0}' "$spec" \
            | grep -oE 'Buggy[A-Za-z0-9]+' | sort -u)"
        for const in $consts; do
            if ! grep -qE "${const}[[:space:]]*=[[:space:]]*TRUE" "$dir"/*.cfgs "$dir"/*.cfg 2>/dev/null; then
                echo "FAIL: $(basename "$spec"): constant ${const} has no check enabling it"
                unexercised=$((unexercised + 1)); fail=1
            fi
        done
    done < <(find "${SPEC_ROOT}" -name 'Antfly*.tla' -not -path '*/.generated/*' | sort)
    [ "$unexercised" -eq 0 ] && echo "-- every Buggy* constant has an enabling check"

    if [ "$fail" -ne 0 ]; then echo "== tla-audit: FAILED"; return 1; fi
    echo "== tla-audit: OK (${#safety_pinned[@]} Safety-pinned check(s) pending migration)"
}

# --- commands --------------------------------------------------------------------

case "${1:-}" in
gate)
    run_audit && run_smoke && run_tier core && run_tier fast && run_negative && \
        echo "== tla-check gate: OK"
    ;;
tier)  run_tier "${2:?usage: tla-check.sh tier <core|fast|heavy|manual>}" ;;
run)
    id="${2:?usage: tla-check.sh run <check-id>}"
    resolve_check "$id" || exit 1
    if [ "$CHECK_TIER" = "negative" ]; then run_expect_fail "$id"; else run_positive "$id"; fi
    ;;
negative) run_negative ;;
smoke)    run_smoke ;;
audit)    run_audit ;;
list)     enumerate | awk -F'\t' '{printf "%-10s %s\n", $4, $1}' | sort ;;
trace)    run_trace "${2:?usage: tla-check.sh trace <family> (with TRACE_FILES=...)}" ;;
*)
    sed -n '2,31p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
