#!/usr/bin/env bash
# Download TLA+ tools (tla2tools.jar and CommunityModules) for model checking
# and trace validation. Uses stamp files to avoid re-downloading.
#
# Usage:
#   source scripts/tla-tools.sh   # sets TLA_JAVA, TLA2TOOLS, COMMUNITY_MODULES
#   bash scripts/tla-tools.sh     # just downloads

set -euo pipefail

TLA_TOOLS_DIR="${TLA_TOOLS_DIR:-${HOME}/.tla-tools}"
TLA2TOOLS_VERSION="${TLA2TOOLS_VERSION:-1.8.0}"
COMMUNITY_MODULES_VERSION="${COMMUNITY_MODULES_VERSION:-202404170853}"

TLA2TOOLS="${TLA_TOOLS_DIR}/tla2tools.jar"
COMMUNITY_MODULES="${TLA_TOOLS_DIR}/CommunityModules-deps.jar"

# Detect Java
detect_java() {
    if [ -n "${JAVA:-}" ]; then
        echo "${JAVA}"
        return
    fi
    if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
        echo "${JAVA_HOME}/bin/java"
        return
    fi
    # macOS TLA+ Toolbox bundled JRE
    local toolbox_java="/Applications/TLA+ Toolbox.app/Contents/Eclipse/plugins/org.lamport.openjdk.macosx.x86_64_14.0.1.7/Contents/Home/bin/java"
    if [ -x "${toolbox_java}" ]; then
        echo "${toolbox_java}"
        return
    fi
    if command -v java &>/dev/null; then
        echo "java"
        return
    fi
    echo "ERROR: Java not found. Install Java 11+ or TLA+ Toolbox." >&2
    return 1
}

download_tools() {
    mkdir -p "${TLA_TOOLS_DIR}"

    local tla2tools_stamp="${TLA_TOOLS_DIR}/.tla2tools-${TLA2TOOLS_VERSION}"
    if [ ! -f "${tla2tools_stamp}" ]; then
        echo "Downloading tla2tools.jar v${TLA2TOOLS_VERSION}..."
        curl -fsSL -o "${TLA2TOOLS}" \
            "https://github.com/tlaplus/tlaplus/releases/download/v${TLA2TOOLS_VERSION}/tla2tools.jar"
        touch "${tla2tools_stamp}"
    fi

    local cm_stamp="${TLA_TOOLS_DIR}/.community-modules-${COMMUNITY_MODULES_VERSION}"
    if [ ! -f "${cm_stamp}" ]; then
        echo "Downloading CommunityModules-deps.jar..."
        curl -fsSL -o "${COMMUNITY_MODULES}" \
            "https://github.com/tlaplus/CommunityModules/releases/latest/download/CommunityModules-deps.jar"
        touch "${cm_stamp}"
    fi
}

TLA_JAVA="$(detect_java)"
download_tools

export TLA_JAVA TLA2TOOLS COMMUNITY_MODULES TLA_TOOLS_DIR
