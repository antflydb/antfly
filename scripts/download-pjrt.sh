#!/usr/bin/env bash
#
# Download PJRT CPU plugin and libtokenizers for XLA backend cross-compilation
#
# This script downloads pre-built libraries for all supported target platforms.
# These are required for building with XLA support (CGO enabled).
#
# Downloads:
#   - PJRT CPU plugin from gomlx/pjrt-cpu-binaries
#   - libtokenizers.a from knights-analytics/hugot (linux-amd64) or daulet/tokenizers (other platforms)
#
# Usage:
#   ./scripts/download-pjrt.sh [PJRT_VERSION]
#
# Example:
#   ./scripts/download-pjrt.sh 0.83.4
#
# The libraries will be downloaded to ./pjrt/<platform>/
# Set PJRT_ROOT environment variable to this directory when building.

set -euo pipefail

# Default versions - update these when upgrading dependencies
PJRT_VERSION="${1:-0.83.4}"
HUGOT_VERSION="${HUGOT_VERSION:-0.5.8}"
TOKENIZERS_VERSION="${TOKENIZERS_VERSION:-1.26.0}"

# Base URLs
PJRT_BASE_URL="https://github.com/gomlx/pjrt-cpu-binaries/releases/download/v${PJRT_VERSION}"
HUGOT_BASE_URL="https://github.com/knights-analytics/hugot/releases/download/v${HUGOT_VERSION}"
TOKENIZERS_BASE_URL="https://github.com/daulet/tokenizers/releases/download/v${TOKENIZERS_VERSION}"

# Output directory
OUTPUT_DIR="${PJRT_ROOT:-./pjrt}"

# List of platforms we support
# Note: PJRT CPU binaries are only available for linux and darwin, x86_64 and arm64
PLATFORMS="linux-amd64 linux-arm64 darwin-arm64"

# Platform mappings: our naming -> PJRT naming
get_pjrt_platform() {
    case "$1" in
        linux-amd64) echo "linux_amd64" ;;
        linux-arm64) echo "linux_arm64" ;;
        darwin-arm64) echo "darwin_arm64" ;;
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

# Download and extract PJRT CPU plugin for a specific platform
download_pjrt() {
    local our_platform="$1"
    local pjrt_platform=$(get_pjrt_platform "$our_platform")
    local archive_name="pjrt_cpu_${pjrt_platform}.tar.gz"
    local url="${PJRT_BASE_URL}/${archive_name}"
    local output_path="${OUTPUT_DIR}/${our_platform}"
    local temp_dir

    info "Downloading PJRT CPU plugin ${PJRT_VERSION} for ${our_platform}..."

    # Create output directory
    mkdir -p "${output_path}/lib"

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

    # Find and copy the PJRT plugin library
    # The archive contains pjrt_c_api_cpu_v<version>_plugin.so (Linux) or .dylib (macOS)
    local plugin_file
    plugin_file=$(find "${temp_dir}" -name "pjrt_c_api_cpu*plugin.*" -type f | head -1)

    if [[ -n "$plugin_file" ]]; then
        cp "$plugin_file" "${output_path}/lib/"
        chmod 755 "${output_path}/lib/$(basename "$plugin_file")"
        info "Successfully installed PJRT CPU plugin for ${our_platform}"
    else
        warn "Could not find PJRT plugin in archive for ${our_platform}"
        ls -la "${temp_dir}"
        trap - EXIT
        rm -rf "${temp_dir}"
        return 1
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

    # For linux-amd64, use hugot's release (it's the only platform they provide)
    # For other platforms, use daulet/tokenizers
    if [[ "$our_platform" == "linux-amd64" ]]; then
        url="${HUGOT_BASE_URL}/libtokenizers.a"
        info "  Using hugot release..."
        if curl -fsSL --retry 3 --retry-delay 2 -o "${output_path}/lib/libtokenizers.a" "${url}"; then
            info "Successfully downloaded libtokenizers.a for ${our_platform}"
            trap - EXIT
            rm -rf "${temp_dir}"
            return 0
        else
            warn "Failed to download from hugot, trying daulet/tokenizers..."
        fi
    fi

    # Use daulet/tokenizers for all platforms (fallback for linux-amd64, primary for others)
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
    local pjrt_ok=0
    local tokenizers_ok=0

    if download_pjrt "${platform}"; then
        pjrt_ok=1
    fi

    if download_tokenizers "${platform}"; then
        tokenizers_ok=1
    fi

    if [[ $pjrt_ok -eq 1 && $tokenizers_ok -eq 1 ]]; then
        return 0
    else
        return 1
    fi
}

# Main function
main() {
    info "PJRT CPU Plugin & Tokenizers Download Script"
    info "PJRT version: ${PJRT_VERSION}"
    info "Hugot version: ${HUGOT_VERSION}"
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
    info "To build with XLA support, set:"
    info "  export PJRT_ROOT=${OUTPUT_DIR}"
    info "  export LD_LIBRARY_PATH=${OUTPUT_DIR}/\$(go env GOOS)-\$(go env GOARCH)/lib:\$LD_LIBRARY_PATH"
    echo ""

    # List what was downloaded
    info "Downloaded libraries:"
    for platform in ${PLATFORMS}; do
        local path="${OUTPUT_DIR}/${platform}"
        local pjrt_lib=""
        local tokenizers_lib=""

        if [[ -d "${path}/lib" ]]; then
            pjrt_lib=$(ls "${path}"/lib/pjrt_c_api_cpu*plugin* 2>/dev/null | head -1 || echo "")
            tokenizers_lib=$(ls "${path}"/lib/libtokenizers* 2>/dev/null | head -1 || echo "")
        fi

        echo -n "  ${platform}: "
        if [[ -n "$pjrt_lib" && -n "$tokenizers_lib" ]]; then
            echo "PJRT ✓ | Tokenizers ✓"
        elif [[ -n "$pjrt_lib" ]]; then
            echo "PJRT ✓ | Tokenizers ✗"
        elif [[ -n "$tokenizers_lib" ]]; then
            echo "PJRT ✗ | Tokenizers ✓"
        else
            echo "PJRT ✗ | Tokenizers ✗"
        fi
    done
}

main "$@"
