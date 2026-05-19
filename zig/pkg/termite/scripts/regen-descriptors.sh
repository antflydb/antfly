#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
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

# Regenerate the protobuf binary descriptor sets (.desc files) that feed
# protoc-zig. Run this whenever an upstream .proto file in protos/ changes.
#
# The source .proto files live under:
#
#   protos/<owner>/<repo>/...
#
# and are checked in so this script is self-contained. The emitted .desc files
# are written next to each consumer's build.zig so the Zig build can
# `addFileArg` them directly.
#
# Requirements:
#   - protoc (Homebrew: `brew install protobuf`)
#
# All imported .proto dependencies are expected to be vendored locally under
# protos/. If a dependency is missing, run scripts/update-vendored-protos.sh.

set -euo pipefail

cd "$(dirname "$0")/.."

PROTOC="${PROTOC:-protoc}"

if ! command -v "$PROTOC" >/dev/null 2>&1; then
    echo "error: $PROTOC not found on PATH" >&2
    exit 1
fi

gen() {
    local out="$1"
    shift
    mkdir -p "$(dirname "$out")"
    echo "  -> $out"
    "$PROTOC" \
        --descriptor_set_out="$out" \
        --include_imports \
        -I protos/protocolbuffers/protobuf/src \
        "$@"
}

echo "regenerating descriptors..."

# ONNX (proto2, `package onnx`)
gen lib/onnx/proto/onnx.desc \
    -I protos/onnx/onnx \
    onnx-ml.proto

# SentencePiece (proto2, `package sentencepiece`)
gen lib/tokenizer/proto/sentencepiece_model.desc \
    -I protos/google/sentencepiece \
    sentencepiece_model.proto

# XLA / HLO (proto3, `package xla`)
gen lib/pjrt/proto/hlo.desc \
    -I protos/openxla/xla \
    xla/service/hlo.proto \
    xla/xla_data.proto \
    xla/service/metrics.proto

echo "done."
