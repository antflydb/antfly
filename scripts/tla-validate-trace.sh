#!/usr/bin/env bash
# Validate ndjson trace files against a TLA+ specification using TLC.
#
# Usage:
#   scripts/tla-validate-trace.sh -s <spec.tla> -c <config.cfg> [-p <parallel>] [-S] <trace files...>
#
# Each trace file is validated independently. TLC reads the trace via the JSON
# environment variable and checks that it constitutes a valid behavior of the
# spec.
#
# Flags:
#   -S  Skip sorting (for specs that require chronological trace order).
#       Default: sort by field 8 (node ID), suitable for multi-raft traces.
#
# Adapted from etcd/raft's validate.sh with macOS compatibility and cleanup.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source tla-tools.sh to get TLA_JAVA, TLA2TOOLS, COMMUNITY_MODULES
source "${SCRIPT_DIR}/tla-tools.sh"

PARALLEL="${PARALLEL:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"
SPEC=""
CONFIG=""
SKIP_SORT=false

show_usage() {
    echo "usage: tla-validate-trace.sh [-p <parallel>] [-S] -s <spec.tla> -c <config.cfg> <trace files...>" >&2
}

while getopts ":hSs:c:p:" flag; do
    case "${flag}" in
        s) SPEC="${OPTARG}" ;;
        c) CONFIG="${OPTARG}" ;;
        p) PARALLEL="${OPTARG}" ;;
        S) SKIP_SORT=true ;;
        h|*) show_usage; exit 1 ;;
    esac
done
shift $((OPTIND - 1))

trace_files=("$@")

if [ -z "${SPEC}" ] || [ -z "${CONFIG}" ] || [ ${#trace_files[@]} -eq 0 ]; then
    show_usage
    exit 1
fi

echo "spec:     ${SPEC}"
echo "config:   ${CONFIG}"
echo "traces:   ${#trace_files[@]} file(s)"
echo "parallel: ${PARALLEL}"
echo ""

STATEDIR="$(mktemp -d)"
trap 'rm -rf "${STATEDIR}"' EXIT

preprocess_trace() {
    local trace="${1}"
    local out="${2}"
    # Strip any non-JSON prefix (e.g., log level/timestamp from structured loggers)
    if [ "${SKIP_SORT}" = "true" ]; then
        sed -E 's/^[^{]+//' "${trace}" > "${out}"
    else
        # Sort by node ID field for multi-raft trace interleaving
        sed -E 's/^[^{]+//' "${trace}" | sort -t'"' -k8 > "${out}"
    fi
}

passed=0
failed=0
total=${#trace_files[@]}

validate_one() {
    local trace="${1}"
    local name
    name="$(basename "${trace}" .ndjson)"

    local preprocessed
    preprocessed="$(mktemp)"
    preprocess_trace "${trace}" "${preprocessed}"

    local state_dir="${STATEDIR}/${name}"
    mkdir -p "${state_dir}"

    if env JSON="${preprocessed}" "${TLA_JAVA}" -XX:+UseParallelGC \
        -cp "${TLA2TOOLS}:${COMMUNITY_MODULES}" \
        tlc2.TLC -config "${CONFIG}" "${SPEC}" \
        -lncheck final -metadir "${state_dir}" -fpmem 0.9 \
        > "${state_dir}/tlc.log" 2>&1; then
        echo "PASS ${trace}"
        rm -f "${preprocessed}"
        return 0
    else
        echo "FAIL ${trace}"
        echo "  TLC output: ${state_dir}/tlc.log"
        rm -f "${preprocessed}"
        return 1
    fi
}

if [ "${PARALLEL}" -le 1 ] || [ ${total} -eq 1 ]; then
    for trace in "${trace_files[@]}"; do
        if validate_one "${trace}"; then
            passed=$((passed + 1))
        else
            failed=$((failed + 1))
        fi
    done
else
    export -f validate_one preprocess_trace
    export TLA_JAVA TLA2TOOLS COMMUNITY_MODULES SPEC CONFIG STATEDIR SKIP_SORT

    results="$(mktemp)"
    printf '%s\n' "${trace_files[@]}" | \
        xargs -P "${PARALLEL}" -I{} bash -c '
            if validate_one "$1"; then
                echo "pass"
            else
                echo "fail"
            fi
        ' -- {} >> "${results}"

    passed=$(grep -c "^pass$" "${results}" || true)
    failed=$(grep -c "^fail$" "${results}" || true)
    rm -f "${results}"
fi

echo ""
echo "${passed} of ${total} trace(s) passed"

if [ "${failed}" -gt 0 ]; then
    exit 1
fi
