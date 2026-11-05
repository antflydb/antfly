#!/usr/bin/env bash
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

set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
server="$repo/zig/zig-out/bin/antfly"
grep_binary="$repo/zig/zig-out/bin/antfly-grep"
port=${ANTFLY_SOURCE_SMOKE_PORT:-14389}
[[ "$port" =~ ^[0-9]+$ ]] || { echo 'Invalid smoke port' >&2; exit 2; }
[[ -x "$server" && -x "$grep_binary" ]] || {
  echo 'Build antfly and antfly-grep in zig/zig-out first.' >&2; exit 2;
}
if lsof -tiTCP:"$port" -sTCP:LISTEN >/dev/null; then
  echo "Smoke port $port is already in use" >&2; exit 2
fi
state=$(mktemp -d "${TMPDIR:-/tmp}/antfly-native-sources.XXXXXX")
server_pid=
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [[ ${ANTFLY_SOURCE_SMOKE_KEEP_STATE:-0} == 1 ]]; then
    echo "Smoke state and server log: $state"
  else
    rm -rf "$state"
  fi
}
trap cleanup EXIT
mkdir -p "$state/models" "$state/ml"
cat >"$state/config.json" <<JSON
{"remote_content":{"security":{"block_private_ips":false}},"public_inference":{"enabled":true},"inference":{"models_dir":"$state/models","ml_dir":"$state/ml"}}
JSON
"$server" standalone --config "$state/config.json" --host 127.0.0.1 --port "$port" \
  --data-dir "$state/data" --control-tick-ms 5 --health false \
  --replica-root-dir "$state/replicas" --replica-catalog-path "$state/catalog.txt" \
  --snapshot-root-dir "$state/snapshots" >"$state/server.log" 2>&1 &
server_pid=$!
ready=0
for ((attempt=0; attempt<120; attempt++)); do
  kill -0 "$server_pid" 2>/dev/null || { cat "$state/server.log" >&2; exit 1; }
  if curl -fsS "http://127.0.0.1:$port/db/v1/tables" >/dev/null 2>&1; then ready=1; break; fi
  sleep 0.25
done
[[ "$ready" == 1 ]] || { cat "$state/server.log" >&2; exit 1; }
export ANTFLY_SOURCE_TEST_ENDPOINT="http://127.0.0.1:$port/db/v1"
export ANTFLY_SOURCES_E2E_URL="$ANTFLY_SOURCE_TEST_ENDPOINT"
export ANTFLY_SOURCES_E2E_RECORDS=${ANTFLY_SOURCES_E2E_RECORDS:-10}
export ANTFLY_GREP_TEST_BINARY="$grep_binary"
export ANTFLY_SOURCE_TEST_VIDEO="$repo/zig/lib/media/testdata/video-audio.mp4"
cd "$repo/go/pkg/sources"
GOWORK=off GOEXPERIMENT=simd go test . \
  -run "${ANTFLY_SOURCE_SMOKE_TESTS:-^Test(LocalAntflySourcePipeline|SourceMediaArtifactIntegration|SourceNativeMultimodalIntegration|NativeCandidateBoundIntegration|NativeIndexedCertificateIntegration|NativeMemberSourceAuthorityIntegration|NativeMemberNavigationIntegration|NativeMemberSnapshotCompletionIntegration|NativeMemberBatchIntegration)$}" \
  -count=1 -v -timeout 10m
