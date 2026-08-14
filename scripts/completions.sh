#!/usr/bin/env bash
set -euo pipefail

output_dir="${1:-completions}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
zig_exe="${ANTFLY_ZIG:-zig}"

rm -rf "$output_dir"
mkdir -p "$output_dir"

# Use a host tool that imports the exact same command specification and
# renderer as `antfly completion`. Release builds may target another OS or CPU,
# so executing the just-built target binary here is not always possible.
for shell in bash zsh fish; do
  (
    cd "$repo_root/zig"
    "$zig_exe" run pkg/antfly/src/completion_generator.zig -- "$shell"
  ) >"$output_dir/antfly.$shell"
done
