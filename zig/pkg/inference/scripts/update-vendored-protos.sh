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

# Refresh vendored upstream .proto sources under protos/<owner>/<repo>/...
#
# By default this updates every vendored proto family. Pass one or more target
# names to refresh a subset:
#
#   scripts/update-vendored-protos.sh protobuf_wkt
#
# Supported targets:
#   onnx sentencepiece openxla protobuf_wkt
#
# Upstream refs are configurable via env vars. For reproducible updates, pass
# explicit refs when you run the script and commit the resulting tree:
#
#   ONNX_REF=<sha-or-tag> OPENXLA_REF=<sha-or-tag> ...
#
# Protobuf well-known types also support local copy mode:
#
#   PROTOBUF_LOCAL_INCLUDE=/path/to/google/protobuf \
#   scripts/update-vendored-protos.sh protobuf_wkt

set -euo pipefail

cd "$(dirname "$0")/.."

CURL="${CURL:-curl}"
ONNX_REF="${ONNX_REF:-main}"
SENTENCEPIECE_REF="${SENTENCEPIECE_REF:-master}"
OPENXLA_REF="${OPENXLA_REF:-main}"
PROTOBUF_REF="${PROTOBUF_REF:-main}"
PROTOBUF_LOCAL_INCLUDE="${PROTOBUF_LOCAL_INCLUDE:-}"

download() {
    local url="$1"
    local dest="$2"
    mkdir -p "$(dirname "$dest")"
    echo "  -> $dest"
    "$CURL" -fsSL "$url" -o "$dest"
}

copy_file() {
    local src="$1"
    local dest="$2"
    mkdir -p "$(dirname "$dest")"
    echo "  -> $dest"
    cp "$src" "$dest"
}

copy_file_strip_go_package() {
    local src="$1"
    local dest="$2"
    mkdir -p "$(dirname "$dest")"
    echo "  -> $dest"
    sed '/^option go_package = /d' "$src" > "$dest"
}

TARGETS=("$@")

has_target() {
    local target="$1"
    if [ "${#TARGETS[@]}" -eq 0 ]; then
        return 0
    fi
    local requested
    for requested in "${TARGETS[@]}"; do
        if [ "$requested" = "$target" ]; then
            return 0
        fi
    done
    return 1
}

validate_targets() {
    local requested
    for requested in "${TARGETS[@]}"; do
        case "$requested" in
            onnx|sentencepiece|openxla|protobuf_wkt) ;;
            *)
                echo "error: unknown target '$requested'" >&2
                echo "known targets: onnx sentencepiece openxla protobuf_wkt" >&2
                exit 1
                ;;
        esac
    done
}

if [ "${#TARGETS[@]}" -gt 0 ]; then
    validate_targets
fi

echo "updating vendored protos..."

if has_target onnx; then
    echo "onnx @ ${ONNX_REF}"
    download \
        "https://raw.githubusercontent.com/onnx/onnx/${ONNX_REF}/onnx/onnx-ml.proto" \
        "protos/onnx/onnx/onnx-ml.proto"
fi

if has_target sentencepiece; then
    echo "sentencepiece @ ${SENTENCEPIECE_REF}"
    download \
        "https://raw.githubusercontent.com/google/sentencepiece/${SENTENCEPIECE_REF}/src/sentencepiece_model.proto" \
        "protos/google/sentencepiece/sentencepiece_model.proto"
fi

if has_target openxla; then
    echo "openxla/xla @ ${OPENXLA_REF}"
    download \
        "https://raw.githubusercontent.com/openxla/xla/${OPENXLA_REF}/xla/service/hlo.proto" \
        "protos/openxla/xla/xla/service/hlo.proto"
    download \
        "https://raw.githubusercontent.com/openxla/xla/${OPENXLA_REF}/xla/service/metrics.proto" \
        "protos/openxla/xla/xla/service/metrics.proto"
    download \
        "https://raw.githubusercontent.com/openxla/xla/${OPENXLA_REF}/xla/xla_data.proto" \
        "protos/openxla/xla/xla/xla_data.proto"
fi

if has_target protobuf_wkt; then
    echo "protobuf well-known types @ ${PROTOBUF_REF}"
    if [ -n "${PROTOBUF_LOCAL_INCLUDE}" ] && [ -d "${PROTOBUF_LOCAL_INCLUDE}" ]; then
        copy_file \
            "${PROTOBUF_LOCAL_INCLUDE}/any.proto" \
            "protos/protocolbuffers/protobuf/src/google/protobuf/any.proto"
        copy_file \
            "${PROTOBUF_LOCAL_INCLUDE}/duration.proto" \
            "protos/protocolbuffers/protobuf/src/google/protobuf/duration.proto"
        copy_file \
            "${PROTOBUF_LOCAL_INCLUDE}/timestamp.proto" \
            "protos/protocolbuffers/protobuf/src/google/protobuf/timestamp.proto"
    else
        download \
            "https://raw.githubusercontent.com/protocolbuffers/protobuf/${PROTOBUF_REF}/src/google/protobuf/any.proto" \
            "protos/protocolbuffers/protobuf/src/google/protobuf/any.proto"
        download \
            "https://raw.githubusercontent.com/protocolbuffers/protobuf/${PROTOBUF_REF}/src/google/protobuf/duration.proto" \
            "protos/protocolbuffers/protobuf/src/google/protobuf/duration.proto"
        download \
            "https://raw.githubusercontent.com/protocolbuffers/protobuf/${PROTOBUF_REF}/src/google/protobuf/timestamp.proto" \
            "protos/protocolbuffers/protobuf/src/google/protobuf/timestamp.proto"
    fi
fi

echo "done."
