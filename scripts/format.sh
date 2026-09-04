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

set -euo pipefail

repo_root=$(CDPATH='' cd "$(dirname "$0")/.." && pwd -P)
cd "$repo_root"

mode="write"
if [[ ${1:-} == "--check" ]]; then
  mode="check"
  shift
fi
mode_label=Formatting
if [[ $mode == check ]]; then
  mode_label="Checking"
fi

if [[ $# -eq 0 ]]; then
  set -- zig go python typescript rust
fi

selected() {
  local candidate=$1
  shift
  local requested
  for requested in "$@"; do
    if [[ $requested == "$candidate" ]]; then
      return 0
    fi
  done
  return 1
}

temporary_dir=$(mktemp -d)
trap 'rm -rf "$temporary_dir"' EXIT

tracked_and_untracked() {
  local output=$1
  local pattern=$2
  git ls-files --cached --others --exclude-standard -z -- "$pattern" >"$output"
}

if selected zig "$@"; then
  echo "==> $mode_label Zig"
  tracked_and_untracked "$temporary_dir/zig" '*.zig'
  if [[ -s $temporary_dir/zig ]]; then
    if [[ $mode == check ]]; then
      xargs -0 zig fmt --check <"$temporary_dir/zig"
    else
      xargs -0 zig fmt <"$temporary_dir/zig"
    fi
  fi
fi

if selected go "$@"; then
  echo "==> $mode_label Go"
  tracked_and_untracked "$temporary_dir/go" '*.go'
  if [[ -s $temporary_dir/go ]]; then
    if [[ $mode == check ]]; then
      xargs -0 gofmt -l <"$temporary_dir/go" >"$temporary_dir/go-unformatted"
      if [[ -s $temporary_dir/go-unformatted ]]; then
        echo "Go files need formatting:" >&2
        sed 's/^/  /' "$temporary_dir/go-unformatted" >&2
        exit 1
      fi
    else
      xargs -0 gofmt -w <"$temporary_dir/go"
    fi
  fi
fi

if selected python "$@"; then
  echo "==> $mode_label Python"
  tracked_and_untracked "$temporary_dir/python" '*.py'
  if [[ -s $temporary_dir/python ]]; then
    if [[ $mode == check ]]; then
      xargs -0 uv run --project py/packages/sdk --locked ruff format --check <"$temporary_dir/python"
    else
      xargs -0 uv run --project py/packages/sdk --locked ruff format <"$temporary_dir/python"
    fi
  fi
fi

if selected typescript "$@"; then
  echo "==> $mode_label TypeScript"
  if [[ $mode == check ]]; then
    (cd ts && node scripts/run-pinned-toolchain.mjs pnpm exec biome format .)
  else
    (cd ts && node scripts/run-pinned-toolchain.mjs pnpm exec biome format --write .)
  fi
fi

if selected rust "$@"; then
  echo "==> $mode_label Rust"
  if [[ $mode == check ]]; then
    cargo fmt --manifest-path rs/Cargo.toml --all --check
  else
    cargo fmt --manifest-path rs/Cargo.toml --all
  fi
fi
