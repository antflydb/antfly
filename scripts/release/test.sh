#!/bin/sh
set -eu

REPO_ROOT=$(CDPATH='' cd "$(dirname "$0")/../.." && pwd -P)
cd "$REPO_ROOT"

policy_python=${ANTFLY_POLICY_PYTHON:-python3}
zig_required=$(
  "$policy_python" scripts/ci/check_sdk_policy.py --get zig
)
zig_exe=${ANTFLY_ZIG:-zig}
if ! command -v "$zig_exe" >/dev/null 2>&1; then
  echo "release tooling tests require Zig $zig_required (set ANTFLY_ZIG to override)" >&2
  exit 127
fi
zig_version=$("$zig_exe" version)
if [ "$zig_version" != "$zig_required" ]; then
  echo "release tooling tests require Zig $zig_required, found $zig_version" >&2
  exit 2
fi

bash scripts/test_install_download_markers.sh
python3 scripts/test_quickstart_docs.py
python3 -m unittest discover -s scripts/packaging -p 'test_*.py'
python3 -m unittest discover -s scripts/release -p 'test_*.py'
python3 scripts/release/validate_workflow_actions.py
sh -n scripts/install.sh
sh -n scripts/release/install_bootstrap.sh
bash -n scripts/test_install_download_markers.sh
