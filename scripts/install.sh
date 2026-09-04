#!/bin/sh
# Antfly install script
# Based on the Ollama install script approach
set -eu

status() { echo ">>> $*" >&2; }
error() { echo "ERROR: $*" >&2; exit 1; }
warning() { echo "WARNING: $*" >&2; }

TEMP_DIR=$(mktemp -d)
TRANSACTION_ACTIVE=0

rollback_target() {
    KEY="$1"
    TARGET="$2"
    [ -n "$TARGET" ] || return 0

    NEW_TARGET="${TARGET}.antfly-new.$$"
    BACKUP_TARGET="${TARGET}.antfly-backup.$$"
    STATE_DIR="$TEMP_DIR/state/$KEY"

    if [ -f "$STATE_DIR/activated" ]; then
        rm -rf "$TARGET" || warning "Could not remove incomplete installation at $TARGET"
    fi
    if [ -f "$STATE_DIR/backed-up" ] && [ -e "$BACKUP_TARGET" ]; then
        mv "$BACKUP_TARGET" "$TARGET" || warning "Could not restore previous installation at $TARGET"
    fi
    rm -rf "$NEW_TARGET" || true
}

rollback_installation() {
    warning "Installation failed; restoring the previous Antfly installation"
    rollback_target fish "${FISH_TARGET:-}"
    rollback_target zsh "${ZSH_TARGET:-}"
    rollback_target bash "${BASH_TARGET:-}"
    rollback_target lib "${LIB_TARGET:-}"
    rollback_target binary "${BIN_TARGET:-}"
}

cleanup() {
    EXIT_CODE=$?
    trap - EXIT HUP INT TERM
    if [ "$TRANSACTION_ACTIVE" -eq 1 ]; then
        rollback_installation
    fi
    rm -rf "$TEMP_DIR"
    exit "$EXIT_CODE"
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

available() { command -v "$1" >/dev/null; }

require() {
    local MISSING=''
    for TOOL in "$@"; do
        if ! available "$TOOL"; then
            MISSING="$MISSING $TOOL"
        fi
    done

    if [ -n "$MISSING" ]; then
        error "Missing required tools:$MISSING. Please install them and try again."
    fi
}

download_archive() {
    case "${ANTFLY_DOWNLOAD_CLASS:-}" in
        employee|ci)
            curl -A "antfly-installer/1" -H "X-Antfly-Audience: ${ANTFLY_DOWNLOAD_CLASS}" \
                --max-redirs 0 "$@"
            ;;
        ""|external)
            curl -A "antfly-installer/1" "$@"
            ;;
        *)
            warning "Ignoring invalid ANTFLY_DOWNLOAD_CLASS; expected external, employee, or ci"
            curl -A "antfly-installer/1" "$@"
            ;;
    esac
}

verify_archive_checksum() {
    ARCHIVE_PATH="$1"
    CHECKSUMS_PATH="$2"
    ARCHIVE_NAME="$3"
    EXPECTED_SHA256=""

    while read -r CHECKSUM FILE_NAME; do
        if [ "$FILE_NAME" = "$ARCHIVE_NAME" ]; then
            EXPECTED_SHA256="$CHECKSUM"
            break
        fi
    done < "$CHECKSUMS_PATH"

    case "$EXPECTED_SHA256" in
        ''|*[!0-9a-fA-F]*) error "Release checksums do not contain a valid SHA-256 for $ARCHIVE_NAME" ;;
    esac
    if [ "${#EXPECTED_SHA256}" -ne 64 ]; then
        error "Release checksums contain an invalid SHA-256 for $ARCHIVE_NAME"
    fi

    if available sha256sum; then
        set -- $(sha256sum "$ARCHIVE_PATH")
        ACTUAL_SHA256="$1"
    elif available shasum; then
        set -- $(shasum -a 256 "$ARCHIVE_PATH")
        ACTUAL_SHA256="$1"
    else
        error "Checksum verification requires sha256sum or shasum"
    fi

    if [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]; then
        error "Checksum verification failed for $ARCHIVE_NAME"
    fi
    status "Verified SHA-256 for $ARCHIVE_NAME"
}

prepare_target() {
    KEY="$1"
    SOURCE="$2"
    TARGET="$3"
    NEW_TARGET="${TARGET}.antfly-new.$$"
    BACKUP_TARGET="${TARGET}.antfly-backup.$$"
    STATE_DIR="$TEMP_DIR/state/$KEY"

    if [ -e "$NEW_TARGET" ] || [ -e "$BACKUP_TARGET" ]; then
        error "Refusing to overwrite an existing installer staging path for $TARGET"
    fi

    mkdir -p "$STATE_DIR" "$(dirname "$TARGET")"
    if [ -d "$SOURCE" ]; then
        mkdir "$NEW_TARGET"
        cp -R "$SOURCE/." "$NEW_TARGET/"
    else
        cp "$SOURCE" "$NEW_TARGET"
    fi
    printf '%s\n' "$TARGET" > "$STATE_DIR/target"
}

activate_target() {
    KEY="$1"
    TARGET="$2"
    NEW_TARGET="${TARGET}.antfly-new.$$"
    BACKUP_TARGET="${TARGET}.antfly-backup.$$"
    STATE_DIR="$TEMP_DIR/state/$KEY"

    if [ -e "$TARGET" ] || [ -L "$TARGET" ]; then
        mv "$TARGET" "$BACKUP_TARGET"
        : > "$STATE_DIR/backed-up"
    fi
    mv "$NEW_TARGET" "$TARGET"
    : > "$STATE_DIR/activated"
}

finalize_target() {
    KEY="$1"
    TARGET="$2"
    STATE_DIR="$TEMP_DIR/state/$KEY"
    BACKUP_TARGET="${TARGET}.antfly-backup.$$"

    rm -rf "$BACKUP_TARGET"
    rm -rf "$STATE_DIR"
}

# Detect OS and architecture
detect_platform() {
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)

    case "$OS" in
        linux) OS="Linux" ;;
        darwin) OS="Darwin" ;;
        *) error "Unsupported operating system: $OS" ;;
    esac

    case "$ARCH" in
        x86_64|amd64) ARCH="x86_64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        *) error "Unsupported architecture: $ARCH" ;;
    esac

    # Darwin only supports arm64 (Apple Silicon)
    if [ "$OS" = "Darwin" ] && [ "$ARCH" = "x86_64" ]; then
        error "macOS x86_64 is not supported. Apple Silicon (arm64) is required."
    fi

    echo "$OS $ARCH"
}

ANTFLY_MIN_GLIBC="2.28"

version_at_least() {
    ACTUAL_VERSION="$1"
    MINIMUM_VERSION="$2"

    case "$ACTUAL_VERSION:$MINIMUM_VERSION" in
        *.*:*.*) ;;
        *) return 1 ;;
    esac

    ACTUAL_MAJOR="${ACTUAL_VERSION%%.*}"
    ACTUAL_REST="${ACTUAL_VERSION#*.}"
    ACTUAL_MINOR="${ACTUAL_REST%%.*}"
    MINIMUM_MAJOR="${MINIMUM_VERSION%%.*}"
    MINIMUM_MINOR="${MINIMUM_VERSION#*.}"

    for VERSION_COMPONENT in "$ACTUAL_MAJOR" "$ACTUAL_MINOR" "$MINIMUM_MAJOR" "$MINIMUM_MINOR"; do
        case "$VERSION_COMPONENT" in
            ''|*[!0-9]*) return 1 ;;
        esac
    done

    [ "$ACTUAL_MAJOR" -gt "$MINIMUM_MAJOR" ] || {
        [ "$ACTUAL_MAJOR" -eq "$MINIMUM_MAJOR" ] &&
            [ "$ACTUAL_MINOR" -ge "$MINIMUM_MINOR" ]
    }
}

select_linux_archive_libc() {
    case "${ANTFLY_LIBC:-auto}" in
        gnu|musl) echo "$ANTFLY_LIBC"; return ;;
        auto|'') ;;
        *) error "ANTFLY_LIBC must be auto, gnu, or musl" ;;
    esac

    if available getconf; then
        GLIBC_INFO=$(getconf GNU_LIBC_VERSION 2>/dev/null || true)
        case "$GLIBC_INFO" in
            'glibc '*)
                GLIBC_VERSION="${GLIBC_INFO#glibc }"
                if version_at_least "$GLIBC_VERSION" "$ANTFLY_MIN_GLIBC"; then
                    echo "gnu"
                else
                    warning "glibc $GLIBC_VERSION is older than the supported GNU archive floor ($ANTFLY_MIN_GLIBC); using the portable musl build"
                    echo "musl"
                fi
                return
                ;;
        esac
    fi
    if available ldd && ldd --version 2>&1 | grep -qi musl; then
        echo "musl"
        return
    fi

    warning "Could not detect the Linux libc; using the portable musl build"
    echo "musl"
}

# Download and install antfly
install_antfly() {
    require curl tar

    status "Detecting platform..."
    read -r OS ARCH <<EOF
$(detect_platform)
EOF
    status "Detected platform: $OS $ARCH"

    VERSION="${1:-latest}"

    # Handle 'latest' version
    if [ "$VERSION" = "latest" ]; then
        status "Fetching latest version..."
        LATEST_URL="https://releases.antfly.io/antfly/latest/metadata.json"
        if VERSION_INFO=$(curl -fsSL "$LATEST_URL" 2>/dev/null); then
            VERSION=$(echo "$VERSION_INFO" | grep -o '"tag":"[^"]*"' | head -1 | cut -d'"' -f4)
        fi
        if [ -z "$VERSION" ] || [ "$VERSION" = "latest" ]; then
            error "Could not determine latest version. Please specify a version explicitly."
        fi
    fi

    # Normalize version: TAG has v prefix, VERSION_NUM does not.
    # Release paths use TAG, while archive filenames use VERSION_NUM.
    case "$VERSION" in
        v*) TAG="$VERSION"; VERSION_NUM="${VERSION#v}" ;;
        *)  TAG="v$VERSION"; VERSION_NUM="$VERSION" ;;
    esac

    status "Installing Antfly $TAG..."

    ARCHIVE_SUFFIX=""
    if [ "$OS" = "Linux" ]; then
        LINUX_ARCHIVE_LIBC=$(select_linux_archive_libc)
        status "Selected Linux archive libc: $LINUX_ARCHIVE_LIBC"
        if [ "$LINUX_ARCHIVE_LIBC" = "gnu" ]; then
            ARCHIVE_SUFFIX="_gnu"
        fi
    fi
    ARCHIVE_NAME="antfly_${VERSION_NUM}_${OS}_${ARCH}${ARCHIVE_SUFFIX}.tar.gz"
    DOWNLOAD_URL="https://releases.antfly.io/antfly/${TAG}/${ARCHIVE_NAME}"
    CHECKSUMS_URL="https://releases.antfly.io/antfly/${TAG}/antfly_zig_checksums.txt"

    status "Downloading from $DOWNLOAD_URL..."
    if ! download_archive -fsSL "$DOWNLOAD_URL" -o "$TEMP_DIR/$ARCHIVE_NAME"; then
        if [ "$OS" = "Linux" ] && [ "${LINUX_ARCHIVE_LIBC:-}" = "gnu" ] && [ "${ANTFLY_LIBC:-auto}" = "auto" ]; then
            warning "GNU archive is unavailable; falling back to the portable musl build"
            ARCHIVE_NAME="antfly_${VERSION_NUM}_${OS}_${ARCH}.tar.gz"
            DOWNLOAD_URL="https://releases.antfly.io/antfly/${TAG}/${ARCHIVE_NAME}"
            status "Downloading from $DOWNLOAD_URL..."
            download_archive -fsSL "$DOWNLOAD_URL" -o "$TEMP_DIR/$ARCHIVE_NAME" || \
                error "Failed to download Antfly. Please check your internet connection and the version number."
        else
            error "Failed to download Antfly. Please check your internet connection and the version number."
        fi
    fi

    status "Downloading release checksums..."
    if ! download_archive -fsSL "$CHECKSUMS_URL" -o "$TEMP_DIR/antfly_zig_checksums.txt"; then
        error "Failed to download release checksums for $TAG. Installation was not changed."
    fi
    verify_archive_checksum \
        "$TEMP_DIR/$ARCHIVE_NAME" \
        "$TEMP_DIR/antfly_zig_checksums.txt" \
        "$ARCHIVE_NAME"

    status "Extracting archive..."
    EXTRACT_DIR="$TEMP_DIR/extracted"
    mkdir "$EXTRACT_DIR"
    tar -xzf "$TEMP_DIR/$ARCHIVE_NAME" -C "$EXTRACT_DIR"

    if [ ! -f "$EXTRACT_DIR/antfly" ]; then
        error "Release archive does not contain the antfly binary"
    fi

    # Determine install location
    if [ "$(id -u)" -eq 0 ]; then
        # Running as root
        INSTALL_DIR="/usr/local/bin"
        LIB_DIR="/usr/local/lib/antfly"
    else
        # Running as regular user
        INSTALL_DIR="$HOME/.local/bin"
        LIB_DIR="$HOME/.local/lib/antfly"
    fi

    BIN_TARGET="$INSTALL_DIR/antfly"
    LIB_TARGET=""
    BASH_TARGET=""
    ZSH_TARGET=""
    FISH_TARGET=""

    chmod +x "$EXTRACT_DIR/antfly"
    if [ -d "$EXTRACT_DIR/lib" ]; then
        LIB_TARGET="$LIB_DIR"
    fi
    if [ -f "$EXTRACT_DIR/completions/antfly.bash" ]; then
        BASH_COMPLETION_DIR="${BASH_COMPLETION_USER_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/bash-completion}/completions"
        BASH_TARGET="$BASH_COMPLETION_DIR/antfly"
    fi
    if [ -f "$EXTRACT_DIR/completions/antfly.zsh" ]; then
        ZSH_COMPLETION_DIR="${ZSH_COMPLETION_USER_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/zsh/site-functions}"
        ZSH_TARGET="$ZSH_COMPLETION_DIR/_antfly"
    fi
    if [ -f "$EXTRACT_DIR/completions/antfly.fish" ]; then
        FISH_COMPLETION_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/fish/completions"
        FISH_TARGET="$FISH_COMPLETION_DIR/antfly.fish"
    fi

    # Prepare every target before replacing any existing installation. If
    # preparation or activation fails, the EXIT trap restores all old targets.
    TRANSACTION_ACTIVE=1
    prepare_target binary "$EXTRACT_DIR/antfly" "$BIN_TARGET"
    [ -z "$LIB_TARGET" ] || prepare_target lib "$EXTRACT_DIR/lib" "$LIB_TARGET"
    [ -z "$BASH_TARGET" ] || prepare_target bash "$EXTRACT_DIR/completions/antfly.bash" "$BASH_TARGET"
    [ -z "$ZSH_TARGET" ] || prepare_target zsh "$EXTRACT_DIR/completions/antfly.zsh" "$ZSH_TARGET"
    [ -z "$FISH_TARGET" ] || prepare_target fish "$EXTRACT_DIR/completions/antfly.fish" "$FISH_TARGET"

    status "Activating Antfly $TAG..."
    activate_target binary "$BIN_TARGET"
    [ -z "$LIB_TARGET" ] || activate_target lib "$LIB_TARGET"
    [ -z "$BASH_TARGET" ] || activate_target bash "$BASH_TARGET"
    [ -z "$ZSH_TARGET" ] || activate_target zsh "$ZSH_TARGET"
    [ -z "$FISH_TARGET" ] || activate_target fish "$FISH_TARGET"

    # Every new target is now active. Commit before removing backups so a
    # cleanup failure cannot trigger a rollback after an earlier backup was
    # already deleted and leave a mixed installation behind.
    TRANSACTION_ACTIVE=0
    finalize_target binary "$BIN_TARGET"
    [ -z "$LIB_TARGET" ] || finalize_target lib "$LIB_TARGET"
    [ -z "$BASH_TARGET" ] || finalize_target bash "$BASH_TARGET"
    [ -z "$ZSH_TARGET" ] || finalize_target zsh "$ZSH_TARGET"
    [ -z "$FISH_TARGET" ] || finalize_target fish "$FISH_TARGET"

    status "Antfly installation complete!"
    status "Installed antfly to $BIN_TARGET"

    # Check if install dir is in PATH
    case ":$PATH:" in
        *":$INSTALL_DIR:"*) ;;
        *)
            warning "$INSTALL_DIR is not in your PATH"
            warning "Run this command before continuing:"
            warning "  export PATH=\"$INSTALL_DIR:\$PATH\""
            ;;
    esac

    status "Run '$BIN_TARGET --help' to get started"
}

# Main execution
main() {
    VERSION=""

    while [ $# -gt 0 ]; do
        case "$1" in
            -h|--help)
                cat <<EOF
Antfly Installer

Usage:
  curl -fsSL https://releases.antfly.io/antfly/latest/install.sh | sh
  curl -fsSL https://releases.antfly.io/antfly/latest/install.sh | sh -s -- v0.2.0

Options:
  -h, --help    Show this help message
  [version]     Install a specific version (e.g., v0.2.0)
                If not specified, installs the latest version.

Environment:
  This script will automatically detect your OS and architecture,
  download the appropriate binaries, and install them.

  ANTFLY_LIBC overrides Linux archive selection. Supported values are auto
  (default), gnu, and musl. Auto uses GNU for glibc $ANTFLY_MIN_GLIBC or newer
  and the portable musl build for older or unknown Linux libc versions.

  ANTFLY_DOWNLOAD_CLASS supplies a best-effort analytics label for archive
  requests. Set it to employee for staff downloads or ci for automated jobs;
  unset, external, and invalid values are measured as external traffic. Values
  are caller-asserted and can be omitted or forged. They are not authentication
  credentials and must not be treated as proof of employee or CI identity.
  Labeled requests reject redirects so their metric headers stay on the
  versioned releases.antfly.io request.

  By default, it installs to:
    - /usr/local/bin (if running as root)
    - ~/.local/bin (if running as regular user)

  Release archives are verified using the SHA-256 published with the release.

For more information, visit: https://docs.antfly.io
EOF
                exit 0
                ;;
            *)
                VERSION="$1"
                shift
                ;;
        esac
    done

    install_antfly "${VERSION:-latest}"
}

main "$@"
