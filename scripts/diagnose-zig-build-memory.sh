#!/usr/bin/env bash
# Measure memory use for a Zig build and optionally capture a macOS stack sample
# when the compiler crosses a resident-memory threshold.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/diagnose-zig-build-memory.sh [options] [-- extra zig build args...]

Options:
  --zig-dir DIR          Directory containing build.zig. Default: zig
  --out-dir DIR          Directory for logs. Default: /tmp/antfly-zig-build-memory
  --prefix DIR           Zig install prefix. Default: /tmp/antfly-zig-build-memory-prefix
  --target TARGET        Zig target. Default: aarch64-linux-musl
  --optimize MODE        Zig optimize mode. Default: ReleaseFast
  --install-step STEP    Build step. Default: install-antfly
  --jobs N               Zig build jobs. Default: 1
  --interval SEC         Sampling interval. Default: 1
  --sample-threshold MB  Capture one macOS sample at this RSS. Default: 8500
  --sample-seconds SEC   Duration for macOS sample. Default: 10
  --no-stack-sample      Disable macOS sample capture
  --label LABEL          Label used in output file names
  -h, --help             Show this help

Environment:
  ZIG_BIN                Zig executable. Default: zig

Examples:
  scripts/diagnose-zig-build-memory.sh
  scripts/diagnose-zig-build-memory.sh --optimize ReleaseSmall
  scripts/diagnose-zig-build-memory.sh --target native -- --verbose
EOF
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
zig_dir="${repo_root}/zig"
out_dir="/tmp/antfly-zig-build-memory"
prefix="/tmp/antfly-zig-build-memory-prefix"
target="aarch64-linux-musl"
optimize="ReleaseFast"
install_step="install-antfly"
jobs="1"
interval="1"
sample_threshold_mb="8500"
sample_seconds="10"
stack_sample="1"
label=""
zig_bin="${ZIG_BIN:-zig}"
extra_args=()

while [ "$#" -gt 0 ]; do
    case "$1" in
        --zig-dir)
            zig_dir="$2"
            shift 2
            ;;
        --out-dir)
            out_dir="$2"
            shift 2
            ;;
        --prefix)
            prefix="$2"
            shift 2
            ;;
        --target)
            target="$2"
            shift 2
            ;;
        --optimize)
            optimize="$2"
            shift 2
            ;;
        --install-step)
            install_step="$2"
            shift 2
            ;;
        --jobs)
            jobs="$2"
            shift 2
            ;;
        --interval)
            interval="$2"
            shift 2
            ;;
        --sample-threshold)
            sample_threshold_mb="$2"
            shift 2
            ;;
        --sample-seconds)
            sample_seconds="$2"
            shift 2
            ;;
        --no-stack-sample)
            stack_sample="0"
            shift
            ;;
        --label)
            label="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            extra_args=("$@")
            break
            ;;
        *)
            echo "unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [ ! -f "${zig_dir}/build.zig" ]; then
    echo "no build.zig found in ${zig_dir}" >&2
    exit 2
fi

mkdir -p "${out_dir}" "${prefix}"

if [ -z "${label}" ]; then
    safe_target="${target//[^A-Za-z0-9_.-]/_}"
    safe_optimize="${optimize//[^A-Za-z0-9_.-]/_}"
    safe_step="${install_step//[^A-Za-z0-9_.-]/_}"
    label="${safe_step}-${safe_target}-${safe_optimize}-$(date +%Y%m%d-%H%M%S)"
fi

timeline="${out_dir}/${label}.rss.tsv"
summary="${out_dir}/${label}.summary.txt"
build_log="${out_dir}/${label}.build.log"
sample_file="${out_dir}/${label}.sample.txt"
max_command_file="${out_dir}/${label}.max-command.txt"

rss_kb_for_pid() {
    ps -o rss= -p "$1" 2>/dev/null | awk '{print $1}'
}

find_build_exe_pids() {
    ps -Ao pid=,comm=,args= | awk '
        /zig build-exe/ {
            pid=$1
            print pid
        }
    '
}

rss_mb_from_kb() {
    awk -v kb="$1" 'BEGIN { printf "%.2f", kb / 1024 }'
}

echo "timestamp	elapsed_s	pid	rss_kb	rss_mb	command" > "${timeline}"

start_epoch="$(date +%s)"
sample_taken="0"
sample_pid=""
max_rss_kb="0"
max_pid=""
max_cmd=""

(
    cd "${zig_dir}"
    "${zig_bin}" build "-j${jobs}" "-Dtarget=${target}" "-Doptimize=${optimize}" "${install_step}" --prefix "${prefix}" "${extra_args[@]}"
) >"${build_log}" 2>&1 &
build_pid="$!"

echo "started build pid=${build_pid}" >&2
echo "logs: ${build_log}" >&2
echo "rss:  ${timeline}" >&2

while kill -0 "${build_pid}" 2>/dev/null; do
    now="$(date +%s)"
    elapsed="$((now - start_epoch))"

    while IFS= read -r pid; do
        [ -n "${pid}" ] || continue
        rss_kb="$(rss_kb_for_pid "${pid}" || true)"
        [ -n "${rss_kb}" ] || continue
        cmd="$(ps -o command= -p "${pid}" 2>/dev/null | tr '\t' ' ' || true)"
        rss_mb="$(rss_mb_from_kb "${rss_kb}")"

        printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
            "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${elapsed}" "${pid}" "${rss_kb}" "${rss_mb}" "${cmd}" \
            >> "${timeline}"

        if [ "${rss_kb}" -gt "${max_rss_kb}" ]; then
            max_rss_kb="${rss_kb}"
            max_pid="${pid}"
            max_cmd="${cmd}"
        fi

        threshold_kb="$((sample_threshold_mb * 1024))"
        if [ "${stack_sample}" = "1" ] && [ "${sample_taken}" = "0" ] && [ "${rss_kb}" -ge "${threshold_kb}" ]; then
            if command -v sample >/dev/null 2>&1; then
                sample_taken="1"
                sample_pid="${pid}"
                echo "capturing sample pid=${pid} rss=${rss_mb}MB -> ${sample_file}" >&2
                sample "${pid}" "${sample_seconds}" -file "${sample_file}" >/dev/null 2>&1 || true
            fi
        fi
    done < <(find_build_exe_pids)

    sleep "${interval}"
done

set +e
wait "${build_pid}"
build_status="$?"
set -e

end_epoch="$(date +%s)"
elapsed_total="$((end_epoch - start_epoch))"
max_rss_mb="$(rss_mb_from_kb "${max_rss_kb}")"

{
    echo "label=${label}"
    echo "status=${build_status}"
    echo "elapsed_s=${elapsed_total}"
    echo "zig_dir=${zig_dir}"
    echo "target=${target}"
    echo "optimize=${optimize}"
    echo "install_step=${install_step}"
    echo "jobs=${jobs}"
    echo "max_rss_kb=${max_rss_kb}"
    echo "max_rss_mb=${max_rss_mb}"
    echo "max_pid=${max_pid}"
    echo "sample_taken=${sample_taken}"
    echo "sample_pid=${sample_pid}"
    echo "timeline=${timeline}"
    echo "build_log=${build_log}"
    if [ "${sample_taken}" = "1" ]; then
        echo "sample_file=${sample_file}"
    fi
    echo "max_command_file=${max_command_file}"
    printf 'max_command_head='
    printf '%s' "${max_cmd}" | awk '{ if (length($0) > 240) print substr($0, 1, 240) "..."; else print }'
} > "${summary}"

printf '%s\n' "${max_cmd}" > "${max_command_file}"

cat "${summary}"
exit "${build_status}"
