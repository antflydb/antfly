#!/usr/bin/env bash
# Qualify the checked-out revision, including PR revisions whose workflow is
# dispatched from main. Keep target selection here rather than in workflow YAML.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 audit|runtime" >&2
  exit 2
fi

: "${VOPR_LOCAL_CACHE_DIR:?}"
: "${VOPR_GLOBAL_CACHE_DIR:?}"

case "$1" in
  audit)
    exec python3 tools/run_bounded_zig_build.py --max-rss-cap 23622320128 -- build \
      vopr-determinism-audit -Doptimize=ReleaseSafe --summary all \
      --cache-dir "$VOPR_LOCAL_CACHE_DIR" --global-cache-dir "$VOPR_GLOBAL_CACHE_DIR"
    ;;
  runtime)
    : "${GITHUB_WORKSPACE:?}"
    : "${RUNNER_TEMP:?}"
    exec python3 "$GITHUB_WORKSPACE/scripts/ci/measure_disk_usage.py" \
      --output "$RUNNER_TEMP/vopr-qualify-disk.json" \
      --path /mnt/cache --path "$GITHUB_WORKSPACE" -- \
      python3 tools/run_bounded_zig_build.py --max-rss-cap 23622320128 -- build \
      antfly-raft-transport-test standby-vopr-test vopr-runtime-test \
      restore-admission-vopr-test secrets-vopr-test \
      -Doptimize=ReleaseSafe --summary all \
      --cache-dir "$VOPR_LOCAL_CACHE_DIR" --global-cache-dir "$VOPR_GLOBAL_CACHE_DIR"
    ;;
  *)
    echo "usage: $0 audit|runtime" >&2
    exit 2
    ;;
esac
