#!/bin/sh
set -eu

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <package> <version> <dist-tag> <tarball>" >&2
  exit 2
fi

package_name="$1"
version="$2"
dist_tag="$3"
tarball="$4"

if [ ! -f "$tarball" ]; then
  echo "npm tarball does not exist: $tarball" >&2
  exit 1
fi

# npm versions are immutable. Treat the same tarball at an already-published
# version as success so the release can resume after a partial registry publish.
published_integrity="$(npm view "${package_name}@${version}" dist.integrity 2>/dev/null || true)"
if [ -n "$published_integrity" ]; then
  local_integrity="sha512-$(openssl dgst -sha512 -binary "$tarball" | openssl base64 -A)"
  if [ "$published_integrity" != "$local_integrity" ]; then
    echo "${package_name}@${version} exists with different contents" >&2
    echo "registry: $published_integrity" >&2
    echo "local:    $local_integrity" >&2
    exit 1
  fi
  echo "${package_name}@${version} already has the same tarball; skipping"
  exit 0
fi

npm publish "$tarball" --access public --provenance --tag "$dist_tag"
