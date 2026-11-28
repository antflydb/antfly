#!/bin/bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start or stop the loopback inference and playground servers with
# run.sh [start|stop|status]. Set A to the assets directory, D to the
# span-copy results directory, and PY to a Python with tokenizers.
# BACKEND=cuda defaults to the BF16 and fast-prefill research mode.
# BACKEND=metal RESEARCH=1 selects shared BF16 weights and F32 KV.
# RESEARCH=0 selects F32. BIN can point to a custom inference build.

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
BACKEND=${BACKEND:-cuda}
case "$BACKEND" in
  cuda)
    RESEARCH=${RESEARCH:-1}
    PROCESS_MEMORY_MB=114688
    HOST_MEMORY_GIB=48
    DEVICE_MEMORY_GIB=100
    COMBINED_MEMORY_GIB=104
    ;;
  metal)
    RESEARCH=${RESEARCH:-0}
    if [ "$RESEARCH" = 1 ]; then
      PROCESS_MEMORY_MB=27648
      HOST_MEMORY_GIB=12
      DEVICE_MEMORY_GIB=26
      COMBINED_MEMORY_GIB=27
    else
      PROCESS_MEMORY_MB=147456
      HOST_MEMORY_GIB=96
      DEVICE_MEMORY_GIB=100
      COMBINED_MEMORY_GIB=144
    fi
    ;;
  *) echo "BACKEND must be cuda or metal"; exit 1 ;;
esac
if [ "$RESEARCH" != 0 ] && [ "$RESEARCH" != 1 ]; then
  echo "RESEARCH must be 0 or 1"; exit 1
fi
PROFILE=memorilla-span-cascade
mkdir -p "$STATE"

# macOS has no setsid executable. Use the same process-group lifecycle on
# both platforms and exec in place so the recorded PID is the group leader.
start_detached() {
  exec nohup "$PY" -I -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' "$@"
}

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
    "memory": {"max_bytes": $((PROCESS_MEMORY_MB * 1024 * 1024))},
    "inference": {
      "memory": {
        "max_host_bytes": $((HOST_MEMORY_GIB * 1024 * 1024 * 1024)),
        "max_device_bytes": $((DEVICE_MEMORY_GIB * 1024 * 1024 * 1024)),
        "max_combined_bytes": $((COMBINED_MEMORY_GIB * 1024 * 1024 * 1024)),
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
    foreign=""
    if [ "$BACKEND" = cuda ]; then
      foreign=$(nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader 2>/dev/null || true)
    fi
    if [ -n "$foreign" ] && [ "${FORCE:-0}" != 1 ]; then
      echo "GPU in use, not starting (FORCE=1 overrides):"; echo "$foreign"; exit 1
    fi
    write_config
    (
      if [ "$BACKEND" = metal ]; then
        export ANTFLY_INFERENCE_REQUIRED_BACKEND=metal
        export ANTFLY_MEMORILLA_RESEARCH_BF16_MATRICES="$RESEARCH"
        unset ANTFLY_MEMORILLA_RESEARCH_FAST_PREFILL
      else
        export CUDA_CACHE_PATH=$W/zig/pkg/inference/.zig-cache/cuda
        if [ "$RESEARCH" = 1 ]; then
          export ANTFLY_MEMORILLA_RESEARCH_BF16_MATRICES=1 ANTFLY_MEMORILLA_RESEARCH_FAST_PREFILL=1
        fi
      fi
      cd "$W"
      start_detached "$BIN" run --host 127.0.0.1 --port "$ZIG_PORT" --config "$STATE/config.json" \
        --process-memory-budget-mb "$PROCESS_MEMORY_MB" > "$STATE/zig.log" 2>&1 < /dev/null &
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
      start_detached "$PY" -I "$HERE/server.py" --host 127.0.0.1 --port "$PORT" \
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
