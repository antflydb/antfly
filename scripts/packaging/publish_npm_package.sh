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

# Registry reads fail closed. Trusted publishing authenticates npm publish, not
# arbitrary dist-tag repair commands, so a resumed publish must also observe
# that the requested tag already points at this exact immutable version.
published_integrity="$(python scripts/release/discover_channel_tag.py npm-version \
  --npm-package "$package_name" --npm-version "$version")"
if [ -n "$published_integrity" ]; then
  local_integrity="sha512-$(openssl dgst -sha512 -binary "$tarball" | openssl base64 -A)"
  if [ "$published_integrity" != "$local_integrity" ]; then
    echo "${package_name}@${version} exists with different contents" >&2
    echo "registry: $published_integrity" >&2
    echo "local:    $local_integrity" >&2
    exit 1
  fi
  published_tag="$(python scripts/release/discover_channel_tag.py npm \
    --npm-package "$package_name" --npm-tag "$dist_tag")"
  if [ "$published_tag" != "v${version}" ]; then
    echo "${package_name}@${version} exists, but dist-tag ${dist_tag} points to ${published_tag:-nothing}" >&2
    echo "repair the dist-tag with an authorized npm credential, then resume the release" >&2
    exit 1
  fi
  echo "${package_name}@${version} already has the same tarball and dist-tag; skipping"
  exit 0
fi

npm publish "$tarball" --access public --provenance --tag "$dist_tag"
published_tag="$(python scripts/release/discover_channel_tag.py npm \
  --npm-package "$package_name" --npm-tag "$dist_tag")"
if [ "$published_tag" != "v${version}" ]; then
  echo "npm publish completed but dist-tag ${dist_tag} does not point to v${version}" >&2
  exit 1
fi
