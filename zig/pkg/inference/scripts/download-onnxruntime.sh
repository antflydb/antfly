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

# Download ONNX Runtime and ONNX Runtime GenAI libraries for the current platform.
#
# Usage:
#   ./scripts/download-onnxruntime.sh [ORT_VERSION] [GENAI_VERSION]
#
# Output: ./onnxruntime/<platform>/include/ and ./onnxruntime/<platform>/lib/

set -euo pipefail

ORT_VERSION="${1:-1.24.3}"
GENAI_VERSION="${2:-0.12.1}"
ORT_BASE_URL="https://github.com/microsoft/onnxruntime/releases/download/v${ORT_VERSION}"
GENAI_BASE_URL="https://github.com/microsoft/onnxruntime-genai/releases/download/v${GENAI_VERSION}"
OUTPUT_DIR="${ONNXRUNTIME_ROOT:-./onnxruntime}"

# Detect platform
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "${OS}-${ARCH}" in
    darwin-arm64)
        PLATFORM="darwin-arm64"
        ORT_PLATFORM="osx-arm64"
        GENAI_PLATFORM="osx-arm64"
        LIB_EXT="dylib"
        ;;
    darwin-x86_64)
        PLATFORM="darwin-amd64"
        ORT_PLATFORM="osx-x86_64"
        GENAI_PLATFORM="osx-x86_64"
        LIB_EXT="dylib"
        ;;
    linux-x86_64)
        PLATFORM="linux-amd64"
        ORT_PLATFORM="linux-x64"
        GENAI_PLATFORM="linux-x64"
        LIB_EXT="so"
        ;;
    linux-aarch64)
        PLATFORM="linux-arm64"
        ORT_PLATFORM="linux-aarch64"
        GENAI_PLATFORM="linux-arm64"
        LIB_EXT="so"
        ;;
    *)
        echo "ERROR: Unsupported platform: ${OS}-${ARCH}"
        exit 1
        ;;
esac

DEST="${OUTPUT_DIR}/${PLATFORM}"

# Check if both ORT and GenAI are already present
if [ -d "${DEST}/lib" ] && [ -d "${DEST}/include" ] && \
   ls "${DEST}"/lib/libonnxruntime-genai*.${LIB_EXT}* >/dev/null 2>&1; then
    echo "ONNX Runtime ${ORT_VERSION} + GenAI ${GENAI_VERSION} already downloaded for ${PLATFORM}"
    exit 0
fi

mkdir -p "${DEST}"
TEMP_DIR=$(mktemp -d)
trap "rm -rf ${TEMP_DIR}" EXIT

# --- ONNX Runtime base ---
ORT_ARCHIVE="onnxruntime-${ORT_PLATFORM}-${ORT_VERSION}.tgz"
echo "Downloading ONNX Runtime ${ORT_VERSION} for ${PLATFORM}..."
curl -fsSL --retry 3 -o "${TEMP_DIR}/${ORT_ARCHIVE}" "${ORT_BASE_URL}/${ORT_ARCHIVE}"
tar -xzf "${TEMP_DIR}/${ORT_ARCHIVE}" -C "${TEMP_DIR}"

ORT_EXTRACTED="${TEMP_DIR}/onnxruntime-${ORT_PLATFORM}-${ORT_VERSION}"
cp -r "${ORT_EXTRACTED}/include" "${DEST}/"
cp -r "${ORT_EXTRACTED}/lib" "${DEST}/"

# --- ONNX Runtime GenAI ---
GENAI_ARCHIVE="onnxruntime-genai-${GENAI_VERSION}-${GENAI_PLATFORM}.tar.gz"
echo "Downloading ONNX Runtime GenAI ${GENAI_VERSION} for ${PLATFORM}..."
if curl -fsSL --retry 3 -o "${TEMP_DIR}/${GENAI_ARCHIVE}" "${GENAI_BASE_URL}/${GENAI_ARCHIVE}"; then
    tar -xzf "${TEMP_DIR}/${GENAI_ARCHIVE}" -C "${TEMP_DIR}"
    GENAI_EXTRACTED=$(find "${TEMP_DIR}" -maxdepth 1 -type d -name "onnxruntime-genai*" | head -1)
    if [ -n "${GENAI_EXTRACTED}" ] && [ -d "${GENAI_EXTRACTED}/lib" ]; then
        cp "${GENAI_EXTRACTED}"/lib/libonnxruntime-genai*.${LIB_EXT}* "${DEST}/lib/" 2>/dev/null || true
        # Copy genai headers if present
        if [ -d "${GENAI_EXTRACTED}/include" ]; then
            cp "${GENAI_EXTRACTED}"/include/ort_genai*.h "${DEST}/include/" 2>/dev/null || true
        fi
    fi
else
    echo "WARNING: Failed to download GenAI — generation features will be unavailable"
fi

chmod 755 "${DEST}"/lib/*.dylib* "${DEST}"/lib/*.so* 2>/dev/null || true

echo "Installed ONNX Runtime ${ORT_VERSION} + GenAI ${GENAI_VERSION} to ${DEST}"
echo "  include: ${DEST}/include/"
echo "  lib:     ${DEST}/lib/"
