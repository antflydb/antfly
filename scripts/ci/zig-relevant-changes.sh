#!/usr/bin/env bash
# Decide whether a diff selects Zig test work.
#
# Usage: zig-relevant-changes.sh BASE HEAD -- PATHSPEC...
# Exit 0 when the diff contains a relevant change, 1 when it does not.
#
# Markdown is documentation, not test input: .md/.mdx files are dropped from
# the diff before deciding. Two exceptions stay in scope: fixture READMEs under
# a testdata/ directory (oracle scripts check them) and the files listed in
# DOC_TEST_INPUTS, which Zig tests read directly. Renames are reported as a
# delete plus an add so renaming code to Markdown still counts. If git itself
# fails, the change is treated as relevant so tests run rather than skip.
set -uo pipefail

DOC_TEST_INPUTS=(
  zig/pkg/inference/QUANT_KERNEL_COMPILER.md
)

base="${1:?base revision}"
head="${2:?head revision}"
shift 2
if [ "${1:-}" != "--" ]; then
  echo "usage: $0 BASE HEAD -- PATHSPEC..." >&2
  exit 2
fi
shift

if ! changed="$(git diff --no-renames --name-only "$base" "$head" -- "$@")"; then
  echo "zig-relevant-changes: git diff failed; selecting tests conservatively" >&2
  exit 0
fi

keep="$(printf '%s\n' "$changed" | awk -v inputs="${DOC_TEST_INPUTS[*]}" '
  BEGIN { n = split(inputs, list, " "); for (i = 1; i <= n; i++) allow[list[i]] = 1 }
  $0 == "" { next }
  /\.mdx?$/ && !/(^|\/)testdata\// && !($0 in allow) { next }
  { print }
')"
[ -n "$keep" ]
