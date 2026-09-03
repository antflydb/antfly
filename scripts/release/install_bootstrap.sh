#!/bin/sh
# Stable channel bootstrap. Keep this compatible with every published
# metadata.json version and delegate release-specific behavior to the immutable
# installer stored with that release.
set -eu

BASE_URL="${ANTFLY_RELEASES_URL:-https://releases.antfly.io/antfly}"
REQUESTED_VERSION="${1:-latest}"

case "$REQUESTED_VERSION" in
    latest|-h|--help)
        METADATA=$(curl -fsSL "$BASE_URL/latest/metadata.json")
        TAG=$(printf '%s' "$METADATA" | sed -n 's/.*"tag":"\([^"]*\)".*/\1/p')
        ;;
    v*) TAG="$REQUESTED_VERSION" ;;
    *) TAG="v$REQUESTED_VERSION" ;;
esac

case "$TAG" in
    v[0-9]*.[0-9]*.[0-9]*) ;;
    *)
        echo "ERROR: Could not resolve a valid Antfly release tag" >&2
        exit 1
        ;;
esac
case "$TAG" in
    *[!A-Za-z0-9._-]*)
        echo "ERROR: Antfly release tags cannot contain URL separators" >&2
        exit 1
        ;;
esac

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT HUP INT TERM
curl -fsSL "$BASE_URL/$TAG/install.sh" -o "$TEMP_DIR/install.sh"

if [ "$REQUESTED_VERSION" = latest ]; then
    set -- "$TAG"
fi
sh "$TEMP_DIR/install.sh" "$@"
