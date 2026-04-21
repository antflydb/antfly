#!/usr/bin/env bash
#
# Download ONNX Runtime, ONNX Runtime GenAI, and libtokenizers libraries for cross-compilation
#
# This script downloads pre-built libraries for all supported target platforms.
# These are required for building with ONNX support (CGO enabled).
#
# Downloads:
#   - ONNX Runtime C/C++ libraries from Microsoft (for embeddings, NER, reranking)
#   - ONNX Runtime GenAI libraries from Microsoft (for LLM text generation)
#   - libtokenizers.a from daulet/tokenizers (all platforms)
#
# Usage:
#   ./scripts/download-onnxruntime.sh [ONNXRUNTIME_VERSION] [GENAI_VERSION]
#
# Example:
#   ./scripts/download-onnxruntime.sh 1.24.3 0.12.1
#
# The libraries will be downloaded to ./onnxruntime/<platform>/
# Set ONNXRUNTIME_ROOT environment variable to this directory when building.

set -euo pipefail

# Default versions - update these when upgrading dependencies
ONNXRUNTIME_VERSION="${1:-1.24.3}"
GENAI_VERSION="${2:-0.12.1}"
TOKENIZERS_VERSION="${TOKENIZERS_VERSION:-1.26.0}"

# Base URLs
ONNXRUNTIME_BASE_URL="https://github.com/microsoft/onnxruntime/releases/download/v${ONNXRUNTIME_VERSION}"
GENAI_BASE_URL="https://github.com/microsoft/onnxruntime-genai/releases/download/v${GENAI_VERSION}"
TOKENIZERS_BASE_URL="https://github.com/daulet/tokenizers/releases/download/v${TOKENIZERS_VERSION}"

# Output directory
OUTPUT_DIR="${ONNXRUNTIME_ROOT:-./onnxruntime}"

# List of platforms we support
PLATFORMS="linux-amd64 linux-arm64 darwin-arm64"

# Platform mappings: our naming -> ONNX Runtime naming
get_onnx_platform() {
    case "$1" in
        linux-amd64) echo "linux-x64" ;;
        linux-arm64) echo "linux-aarch64" ;;
        darwin-arm64) echo "osx-arm64" ;;
        *) echo "" ;;
    esac
}

# Platform mappings: our naming -> daulet/tokenizers naming
get_tokenizers_platform() {
    case "$1" in
        linux-amd64) echo "linux-amd64" ;;
        linux-arm64) echo "linux-arm64" ;;
        darwin-arm64) echo "darwin-arm64" ;;
        *) echo "" ;;
    esac
}

# Platform mappings: our naming -> ONNX Runtime GenAI naming
get_genai_platform() {
    case "$1" in
        linux-amd64) echo "linux-x64" ;;
        linux-arm64) echo "linux-arm64" ;;
        darwin-arm64) echo "osx-arm64" ;;
        *) echo "" ;;
    esac
}

# Get library extension for platform
get_lib_extension() {
    case "$1" in
        darwin-*) echo "dylib" ;;
        *) echo "so" ;;
    esac
}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Check for required tools
check_requirements() {
    if ! command -v curl &> /dev/null; then
        error "curl is required but not installed"
    fi
    if ! command -v tar &> /dev/null; then
        error "tar is required but not installed"
    fi
}

# Download and extract ONNX Runtime for a specific platform
download_onnxruntime() {
    local our_platform="$1"
    local onnx_platform=$(get_onnx_platform "$our_platform")
    local archive_name="onnxruntime-${onnx_platform}-${ONNXRUNTIME_VERSION}.tgz"
    local url="${ONNXRUNTIME_BASE_URL}/${archive_name}"
    local output_path="${OUTPUT_DIR}/${our_platform}"
    local temp_dir

    info "Downloading ONNX Runtime ${ONNXRUNTIME_VERSION} for ${our_platform}..."

    # Create output directory
    mkdir -p "${output_path}"

    # Create temp directory for download
    temp_dir=$(mktemp -d)
    trap "rm -rf ${temp_dir}" EXIT

    # Download archive
    if ! curl -fsSL --retry 3 --retry-delay 2 -o "${temp_dir}/${archive_name}" "${url}"; then
        warn "Failed to download ${archive_name} - platform may not be available"
        trap - EXIT
        rm -rf "${temp_dir}"
        return 1
    fi

    # Extract archive
    info "Extracting ${archive_name}..."
    tar -xzf "${temp_dir}/${archive_name}" -C "${temp_dir}"

    # Move files to output directory
    # ONNX Runtime archives extract to onnxruntime-<platform>-<version>/
    local extracted_dir="${temp_dir}/onnxruntime-${onnx_platform}-${ONNXRUNTIME_VERSION}"

    if [[ -d "${extracted_dir}" ]]; then
        # Copy include and lib directories
        cp -r "${extracted_dir}/include" "${output_path}/" 2>/dev/null || true
        cp -r "${extracted_dir}/lib" "${output_path}/" 2>/dev/null || true

        # Some versions have different directory structures
        if [[ -d "${extracted_dir}/onnxruntime" ]]; then
            cp -r "${extracted_dir}/onnxruntime"/* "${output_path}/" 2>/dev/null || true
        fi

        # Ensure shared libraries have correct permissions for downstream packaging
        chmod 755 "${output_path}"/lib/*.so* "${output_path}"/lib/*.dylib* 2>/dev/null || true
        info "Successfully installed ONNX Runtime for ${our_platform}"
    else
        warn "Unexpected archive structure for ${our_platform}"
        ls -la "${temp_dir}"
        trap - EXIT
        rm -rf "${temp_dir}"
        return 1
    fi

    trap - EXIT
    rm -rf "${temp_dir}"
    return 0
}

# Download and extract ONNX Runtime GenAI for a specific platform (for LLM generation)
download_genai() {
    local our_platform="$1"
    local genai_platform=$(get_genai_platform "$our_platform")
    local lib_ext=$(get_lib_extension "$our_platform")
    local archive_name="onnxruntime-genai-${GENAI_VERSION}-${genai_platform}.tar.gz"
    local url="${GENAI_BASE_URL}/${archive_name}"
    local output_path="${OUTPUT_DIR}/${our_platform}"
    local temp_dir

    # GenAI is optional - skip if platform not supported
    if [[ -z "$genai_platform" ]]; then
        warn "ONNX Runtime GenAI not available for ${our_platform}"
        return 1
    fi

    info "Downloading ONNX Runtime GenAI ${GENAI_VERSION} for ${our_platform}..."

    # Create output directory
    mkdir -p "${output_path}/lib"

    # Create temp directory for download
    temp_dir=$(mktemp -d)
    trap "rm -rf ${temp_dir}" EXIT

    # Download archive
    if ! curl -fsSL --retry 3 --retry-delay 2 -o "${temp_dir}/${archive_name}" "${url}"; then
        warn "Failed to download ${archive_name} - GenAI may not be available for this platform"
        trap - EXIT
        rm -rf "${temp_dir}"
        return 1
    fi

    # Extract archive
    info "Extracting ${archive_name}..."
    tar -xzf "${temp_dir}/${archive_name}" -C "${temp_dir}"

    # Find and copy the GenAI libraries
    # GenAI archives typically extract to onnxruntime-genai-<version>-<platform>/lib/
    local extracted_dir
    extracted_dir=$(find "${temp_dir}" -maxdepth 1 -type d -name "onnxruntime-genai*" | head -1)

    if [[ -n "${extracted_dir}" && -d "${extracted_dir}/lib" ]]; then
        # Copy all GenAI libraries
        cp -r "${extracted_dir}"/lib/libonnxruntime-genai*.${lib_ext}* "${output_path}/lib/" 2>/dev/null || true

        # Also copy any bundled onnxruntime library if present (GenAI often includes it)
        cp -r "${extracted_dir}"/lib/libonnxruntime*.${lib_ext}* "${output_path}/lib/" 2>/dev/null || true

        # Ensure shared libraries have correct permissions for downstream packaging
        chmod 755 "${output_path}"/lib/*.so* "${output_path}"/lib/*.dylib* 2>/dev/null || true
        info "Successfully installed ONNX Runtime GenAI for ${our_platform}"
    else
        # Try alternative structure
        local found_lib=$(find "${temp_dir}" -name "libonnxruntime-genai*.${lib_ext}*" -type f | head -1)
        if [[ -n "$found_lib" ]]; then
            cp "$(dirname "$found_lib")"/libonnxruntime-genai*.${lib_ext}* "${output_path}/lib/" 2>/dev/null || true
            cp "$(dirname "$found_lib")"/libonnxruntime*.${lib_ext}* "${output_path}/lib/" 2>/dev/null || true
            chmod 755 "${output_path}"/lib/*.so* "${output_path}"/lib/*.dylib* 2>/dev/null || true
            info "Successfully installed ONNX Runtime GenAI for ${our_platform}"
        else
            warn "Could not find GenAI libraries in archive for ${our_platform}"
            ls -laR "${temp_dir}"
            trap - EXIT
            rm -rf "${temp_dir}"
            return 1
        fi
    fi

    trap - EXIT
    rm -rf "${temp_dir}"
    return 0
}

# Download libtokenizers.a for a specific platform
download_tokenizers() {
    local our_platform="$1"
    local output_path="${OUTPUT_DIR}/${our_platform}"
    local temp_dir
    local url
    local archive_name

    info "Downloading libtokenizers for ${our_platform}..."

    # Create output directory
    mkdir -p "${output_path}/lib"

    temp_dir=$(mktemp -d)
    trap "rm -rf ${temp_dir}" EXIT

    # Use daulet/tokenizers for all platforms (must match Go binding version)
    local tokenizers_platform=$(get_tokenizers_platform "$our_platform")
    archive_name="libtokenizers.${tokenizers_platform}.tar.gz"
    url="${TOKENIZERS_BASE_URL}/${archive_name}"

    if ! curl -fsSL --retry 3 --retry-delay 2 -o "${temp_dir}/${archive_name}" "${url}"; then
        warn "Failed to download libtokenizers for ${our_platform}"
        trap - EXIT
        rm -rf "${temp_dir}"
        return 1
    fi

    # Extract archive
    tar -xzf "${temp_dir}/${archive_name}" -C "${temp_dir}"

    # Find and copy libtokenizers.a
    if [[ -f "${temp_dir}/libtokenizers.a" ]]; then
        cp "${temp_dir}/libtokenizers.a" "${output_path}/lib/"
        info "Successfully installed libtokenizers.a for ${our_platform}"
    else
        # Some archives may have different structure
        local found_lib=$(find "${temp_dir}" -name "libtokenizers.a" -type f | head -1)
        if [[ -n "$found_lib" ]]; then
            cp "$found_lib" "${output_path}/lib/"
            info "Successfully installed libtokenizers.a for ${our_platform}"
        else
            warn "Could not find libtokenizers.a in archive for ${our_platform}"
            trap - EXIT
            rm -rf "${temp_dir}"
            return 1
        fi
    fi

    trap - EXIT
    rm -rf "${temp_dir}"
    return 0
}

# Download all libraries for a platform
download_platform() {
    local platform="$1"
    local onnx_ok=0
    local genai_ok=0
    local tokenizers_ok=0

    if download_onnxruntime "${platform}"; then
        onnx_ok=1
    fi

    if download_genai "${platform}"; then
        genai_ok=1
    fi

    if download_tokenizers "${platform}"; then
        tokenizers_ok=1
    fi

    # Core libraries (ONNX + tokenizers) are required, GenAI is optional
    if [[ $onnx_ok -eq 1 && $tokenizers_ok -eq 1 ]]; then
        return 0
    else
        return 1
    fi
}

# Main function
main() {
    info "ONNX Runtime & Tokenizers Download Script"
    info "ONNX Runtime version: ${ONNXRUNTIME_VERSION}"
    info "Tokenizers version: ${TOKENIZERS_VERSION}"
    info "Output directory: ${OUTPUT_DIR}"
    echo ""

    check_requirements

    # Create output directory
    mkdir -p "${OUTPUT_DIR}"

    local success_count=0
    local fail_count=0

    for platform in ${PLATFORMS}; do
        echo "----------------------------------------"
        if download_platform "${platform}"; then
            success_count=$((success_count + 1))
        else
            fail_count=$((fail_count + 1))
        fi
        echo ""
    done

    echo "========================================"
    info "Download complete!"
    info "Successfully downloaded: ${success_count} platforms"
    if [[ ${fail_count} -gt 0 ]]; then
        warn "Partial/failed downloads: ${fail_count} platforms"
    fi
    echo ""
    info "To build with ONNX support, set:"
    info "  export ONNXRUNTIME_ROOT=${OUTPUT_DIR}"
    echo ""
    info "Then run goreleaser:"
    info "  ONNXRUNTIME_ROOT=${OUTPUT_DIR} SDK_PATH=\$(xcrun --show-sdk-path) goreleaser release --snapshot --clean"
    echo ""

    # List what was downloaded
    info "Downloaded libraries:"
    for platform in ${PLATFORMS}; do
        local path="${OUTPUT_DIR}/${platform}"
        local onnx_lib=""
        local tokenizers_lib=""

        if [[ -d "${path}/lib" ]]; then
            onnx_lib=$(ls "${path}"/lib/libonnxruntime* 2>/dev/null | head -1 || echo "")
            tokenizers_lib=$(ls "${path}"/lib/libtokenizers* 2>/dev/null | head -1 || echo "")
        fi

        echo -n "  ${platform}: "
        if [[ -n "$onnx_lib" && -n "$tokenizers_lib" ]]; then
            echo "ONNX ✓ | Tokenizers ✓"
        elif [[ -n "$onnx_lib" ]]; then
            echo "ONNX ✓ | Tokenizers ✗"
        elif [[ -n "$tokenizers_lib" ]]; then
            echo "ONNX ✗ | Tokenizers ✓"
        else
            echo "ONNX ✗ | Tokenizers ✗"
        fi
    done
}

main "$@"
