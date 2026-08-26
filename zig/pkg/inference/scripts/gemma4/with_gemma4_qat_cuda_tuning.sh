#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/gemma4_qat_cuda_tuning.sh"

if [[ "$#" -eq 0 ]]; then
  echo "usage: $0 COMMAND [ARGS...]" >&2
  exit 2
fi

# The server resets the pinned-temp capture sequence whenever it provisions a
# new request-owned KV storage. Batching stays disabled by default so replay
# state cannot cross concurrently active requests.
case "${ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING:-1}" in
  1|true|yes|on)
    disable_continuous_batching=1
    ;;
  0|false|no|off)
    disable_continuous_batching=0
    ;;
  *)
    echo "ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING must be a boolean" >&2
    exit 2
    ;;
esac
antfly_decode_graph_replay="${ANTFLY_SERVER_DECODE_GRAPH_REPLAY:-required}"
if [[ "$disable_continuous_batching" -eq 0 ]]; then
  case "$antfly_decode_graph_replay" in
    off|0|false)
      ;;
    *)
      echo "continuous batching requires ANTFLY_SERVER_DECODE_GRAPH_REPLAY=off" >&2
      exit 2
      ;;
  esac
fi
# The typed runtime variable is the canonical contract used by benchmark and
# server profiles. Keep the legacy alias as a compatibility fallback only;
# otherwise the wrapper could silently replace a reviewed long-context graph
# capacity with its historical 544-token default.
gemma4_qat_cuda_tuning_env "${ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY:-${ANTFLY_CAPTURE_FORCE_KV_CAPACITY:-544}}"
exec env "${GEMMA4_QAT_CUDA_ENV[@]}" \
  ANTFLY_INFERENCE_DISABLE_CONTINUOUS_BATCHING="$disable_continuous_batching" \
  "$@"
