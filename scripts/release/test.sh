#!/bin/sh
set -eu

REPO_ROOT=$(CDPATH='' cd "$(dirname "$0")/../.." && pwd -P)
cd "$REPO_ROOT"

bash scripts/test_install_download_markers.sh
python3 scripts/test_quickstart_docs.py
python3 -m unittest discover -s scripts/packaging -p 'test_*.py'
python3 -m unittest discover -s scripts/release -p 'test_*.py'
python3 scripts/release/validate_workflow_actions.py
sh -n scripts/install.sh
sh -n scripts/release/install_bootstrap.sh
bash -n scripts/test_install_download_markers.sh
