#!/bin/bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Memorilla playground (plan section 8.35): starts the Zig inference server
# with one hybrid_research profile (span1-35k-cascade over the released stage3
# reader) and the playground page server, both on 127.0.0.1.
#
#   run.sh [start]   start both (the Zig server only if it is not running)
#   run.sh stop      stop both
#   run.sh status    show the PIDs and ports
#
# Environment (defaults in brackets):
#   PORT [8765]           page server port
#   ZIG_PORT [18090]      Zig inference server port
#   BIN                   [zig/pkg/inference/zig-out/bin/antfly-inference-dev]
#                         build: cd zig/pkg/inference && zig build -Dcuda=true -Doptimize=ReleaseFast --prefix zig-out
#   BUNDLE                hybrid span bundle [$D/span1-35k-cascade/hybrid-span-bundle]
#   STATE                 config, logs and PIDs [.benchmark-results/memorilla/playground]
#   RANKING [dense]       dense (server /ai/v1/embed, stock Qwen3-Embedding-4B) or bm25
#   RESEARCH [1]          research mode of the latency studies (BF16 matrices + fast prefill)
#   FORCE [0]             start even if another process holds the GPU
# A, D and PY come from .benchmark-results/memorilla/qwen-span-copy/env.sh.
set -euo pipefail
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
W=$(cd "$HERE/../../../../../.." && pwd)
ENV_SH=$W/.benchmark-results/memorilla/qwen-span-copy/env.sh
[ -e "$ENV_SH" ] && . "$ENV_SH"
: "${A:?set A (benchmark assets) or provide $ENV_SH}" "${D:?set D (span-copy results)}" "${PY:?set PY (python with tokenizers)}"
PORT=${PORT:-8765}
ZIG_PORT=${ZIG_PORT:-18090}
BIN=${BIN:-$W/zig/pkg/inference/zig-out/bin/antfly-inference-dev}
BUNDLE=${BUNDLE:-$D/span1-35k-cascade/hybrid-span-bundle}
STATE=${STATE:-$W/.benchmark-results/memorilla/playground}
RANKING=${RANKING:-dense}
PROFILE=memorilla-span-cascade
mkdir -p "$STATE"

alive() { [ -s "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }

stop_one() {
  local name=$1 pidfile=$STATE/$1.pid
  if alive "$pidfile"; then
    local pid; pid=$(cat "$pidfile")
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -0 "$pid" 2>/dev/null && { kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true; }
    echo "stopped $name ($pid)"
  else
    echo "$name not running"
  fi
  rm -f "$pidfile"
}

status() {
  for name in zig page; do
    if alive "$STATE/$name.pid"; then echo "$name: running, pid $(cat "$STATE/$name.pid")"; else echo "$name: stopped"; fi
  done
  echo "page: http://127.0.0.1:$PORT/   zig: http://127.0.0.1:$ZIG_PORT/ai/v1/models   logs: $STATE/*.log"
}

write_config() {
  cat > "$STATE/config.json" <<EOF
{
  "resources": {
    "memory": {"max_bytes": 120259084288},
    "inference": {
      "memory": {
        "max_host_bytes": 51539607552,
        "max_device_bytes": 107374182400,
        "max_combined_bytes": 111669149696,
        "max_kv_bytes": 4294967296,
        "max_scratch_bytes": 17179869184
      },
      "admission": {"max_concurrent_requests": 2, "max_work_units": 4},
      "request_limits": {
        "max_items_per_request": 64,
        "max_input_tokens_per_item": 2048,
        "max_output_tokens_per_item": 128
      }
    }
  },
  "inference": {
    "models_dir": "$A/managed-f32",
    "ml_dir": "$A/ml-unused",
    "keep_alive": "24h",
    "max_loaded_models": 4,
    "http_max_body_mb": 16,
    "prompt_cache": {"enabled": false},
    "kernel_jit": {"mode": "off", "cache_dir": "$STATE/kernel-jit-cache"},
    "memorilla": {
      "enabled": true,
      "profiles": [{
        "name": "$PROFILE",
        "reader_kind": "hybrid_research",
        "encoder": "$A/managed-f32/encoder",
        "decoder": "$A/managed-f32/decoder",
        "memory_module_path": "$BUNDLE",
        "released_memory_module_path": "$A/bundles/stage3",
        "max_documents": 64,
        "max_new_tokens": 128
      }]
    }
  }
}
EOF
}

start() {
  if alive "$STATE/zig.pid"; then
    echo "zig server already running ($(cat "$STATE/zig.pid"))"
  else
    [ -x "$BIN" ] || { echo "missing $BIN (build it first, see the header)"; exit 1; }
    local foreign
    foreign=$(nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader 2>/dev/null || true)
    if [ -n "$foreign" ] && [ "${FORCE:-0}" != 1 ]; then
      echo "GPU in use, not starting (FORCE=1 overrides):"; echo "$foreign"; exit 1
    fi
    write_config
    (
      export CUDA_CACHE_PATH=$W/zig/pkg/inference/.zig-cache/cuda
      if [ "${RESEARCH:-1}" = 1 ]; then
        export ANTFLY_MEMORILLA_RESEARCH_BF16_MATRICES=1 ANTFLY_MEMORILLA_RESEARCH_FAST_PREFILL=1
      fi
      cd "$W"
      setsid nohup "$BIN" run --host 127.0.0.1 --port "$ZIG_PORT" --config "$STATE/config.json" \
        --process-memory-budget-mb 114688 > "$STATE/zig.log" 2>&1 < /dev/null &
      echo $! > "$STATE/zig.pid"
    )
    echo "zig server starting (pid $(cat "$STATE/zig.pid"), log $STATE/zig.log)"
    for _ in $(seq 1 120); do
      curl -sf "http://127.0.0.1:$ZIG_PORT/ai/v1/models" > /dev/null 2>&1 && break
      alive "$STATE/zig.pid" || { echo "zig server exited; see $STATE/zig.log"; tail -20 "$STATE/zig.log"; exit 1; }
      sleep 1
    done
  fi
  if alive "$STATE/page.pid"; then
    echo "page server already running ($(cat "$STATE/page.pid"))"
  else
    (
      cd "$W"
      setsid nohup "$PY" -I "$HERE/server.py" --host 127.0.0.1 --port "$PORT" \
        --zig-url "http://127.0.0.1:$ZIG_PORT" --profile "$PROFILE" --embed-model encoder \
        --tokenizer "$A/managed-f32/encoder/tokenizer.json" --bundle "$BUNDLE" --ranking "$RANKING" --warm \
        > "$STATE/page.log" 2>&1 < /dev/null &
      echo $! > "$STATE/page.pid"
    )
    sleep 2
    alive "$STATE/page.pid" || { echo "page server exited; see $STATE/page.log"; tail -20 "$STATE/page.log"; exit 1; }
    echo "page server started (pid $(cat "$STATE/page.pid"), log $STATE/page.log); warming the models in the background"
  fi
  status
}

case "${1:-start}" in
  start) start ;;
  stop) stop_one page; stop_one zig ;;
  status) status ;;
  *) echo "usage: $0 [start|stop|status]"; exit 2 ;;
esac
