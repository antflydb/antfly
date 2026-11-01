#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

# Garbage-collect local build caches for the main checkout and every worktree
# under .worktrees/, plus the per-user Zig, Go, Rust, Python (uv) and Node
# caches. Neither Zig nor Cargo evicts build output on its own, so caches grow
# until something deletes them.
#
# What it cleans:
#   Zig    .zig-cache, zig-out*, zig-local in every checkout; Zig-shaped cache
#          dirs under ~/.cache and $TMPDIR; o/ h/ z/ tmp/ in the global cache
#          (fetched packages in p/ are kept)
#   Rust   rs/target (after `cargo sweep` when installed)
#   Go     go clean -cache
#   Python uv cache prune
#   Node   pnpm store prune, npm cache verify
#
# Zig and Rust dirs are removed when idle for GC_MAX_AGE_DAYS (default 3) or
# larger than GC_MAX_GB (default 20). Anything written in the last
# GC_ACTIVE_MINUTES (default 30) is assumed to belong to a running build and
# is skipped -- kept as an extra guard, but not relied on alone: a 1.5-2 hour
# release build can go that long without touching the top of its own cache
# dir. Before touching a worktree's Zig/Rust caches, cache_gc.sh also checks
# (via scripts/cache_gc_activity.sh) whether a zig/maker/cargo/rustc process
# is actually working in that worktree (by cwd or --cache-dir/--prefix/
# --global-cache-dir argument) and skips only that worktree if so, leaving
# every other worktree's caches to be cleaned normally. The Zig global
# cache, Go build cache, uv cache, and Node (pnpm/npm) caches are shared
# across every worktree, so instead they are skipped entirely -- not just
# deferred -- whenever a matching process is running anywhere on the
# machine, regardless of which worktree it is in.
#
# Usage: scripts/cache_gc.sh            (or: make gc)
#        DRY_RUN=1 scripts/cache_gc.sh  (or: make gc DRY_RUN=1)

set -uo pipefail

source "$(dirname "$0")/cache_gc_activity.sh"

DRY_RUN="${DRY_RUN:-0}"
GC_MAX_AGE_DAYS="${GC_MAX_AGE_DAYS:-3}"
GC_MAX_GB="${GC_MAX_GB:-20}"
GC_ACTIVE_MINUTES="${GC_ACTIVE_MINUTES:-30}"

repo_root="${CACHE_GC_REPO_ROOT:-$(cd "$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")" && pwd)}"
user_cache="${XDG_CACHE_HOME:-$HOME/.cache}"
zig_global_cache="${ZIG_GLOBAL_CACHE_DIR:-$user_cache/zig}"
max_kb=$((GC_MAX_GB * 1024 * 1024))
freed_kb=0
skip_log=""

have() { command -v "$1" >/dev/null 2>&1; }

# One worktree root per line: the main checkout plus every `git worktree
# add`-ed tree, wherever it lives. CACHE_GC_WORKTREES lets a test stand in
# a fake tree without a real repo.
worktree_roots() {
	if [ -n "${CACHE_GC_WORKTREES:-}" ]; then
		printf '%s\n' "$CACHE_GC_WORKTREES"
	else
		git -C "$repo_root" worktree list --porcelain 2>/dev/null | awk '/^worktree /{print $2}'
	fi
}

# The most specific (longest) worktree root containing $1, if any. A
# worktree under .worktrees/ sits inside the main checkout's own root, so
# the longest match -- not just "a" match -- is what tells a nested
# worktree's cache apart from the main checkout's.
owning_worktree() {
	local path="$1" best="" root
	while IFS= read -r root; do
		[ -n "$root" ] || continue
		case "$path" in
		"$root" | "$root"/*)
			[ "${#root}" -gt "${#best}" ] && best="$root"
			;;
		esac
	done < <(worktree_roots)
	printf '%s\n' "$best"
}

# Record and print why something was left alone, in both dry-run and real
# mode -- this runs before the DRY_RUN branch in run(), not after it.
note_skip() {
	local what="$1" reason="$2"
	echo "  skip ($reason): $what"
	skip_log="${skip_log}${skip_log:+$'\n'}$what -- $reason"
}

size_kb() { du -sk "$1" 2>/dev/null | awk '{print $1}'; }

human() { awk -v kb="$1" 'BEGIN { printf "%.1fG", kb / 1048576 }'; }

free_kb() { df -Pk "$repo_root" | awk 'NR == 2 { print $4 }'; }

run() {
	if [ "$DRY_RUN" = 1 ]; then
		echo "  [dry-run] $*" >&2
	else
		"$@"
	fi
}

# True when anything near the top of $1 was written recently. Zig touches its
# h/, o/ and tmp/ subdirs on every build, so depth 2 is enough and stays fast
# on caches with hundreds of thousands of files.
is_active() {
	[ -n "$(find "$1" -maxdepth 2 -mmin "-$GC_ACTIVE_MINUTES" -print -quit 2>/dev/null)" ]
}

is_idle() {
	[ -z "$(find "$1" -maxdepth 2 -mtime "-$GC_MAX_AGE_DAYS" -print -quit 2>/dev/null)" ]
}

# Remove a build-output dir if it is idle or over budget, but never while a
# build is writing to it.
gc_dir() {
	local dir="$1" kb reason=""
	[ -d "$dir" ] || return 0
	kb="$(size_kb "$dir")"
	kb="${kb:-0}"
	if is_active "$dir"; then
		note_skip "$dir" "written in last ${GC_ACTIVE_MINUTES}m, $(human "$kb")"
		return 0
	fi
	if [ "$kb" -gt "$max_kb" ]; then
		reason="over ${GC_MAX_GB}G"
	elif is_idle "$dir"; then
		reason="idle ${GC_MAX_AGE_DAYS}d"
	else
		return 0
	fi
	echo "  rm $(human "$kb") ($reason): $dir"
	if run rm -rf "$dir" && [ "$DRY_RUN" != 1 ]; then
		freed_kb=$((freed_kb + kb))
	fi
}

echo "==> Zig build caches under $repo_root"
while IFS= read -r -d '' dir; do
	wt="$(owning_worktree "$dir")"
	if reason="$(build_active_in "${wt:-$dir}")"; then
		note_skip "$dir" "active build, $reason"
		continue
	fi
	gc_dir "$dir"
done < <(find "$repo_root" \
	\( -name node_modules -o -name .git -o -name target \) -prune -o \
	-type d \( -name .zig-cache -o -name 'zig-out*' -o -name zig-local \) -prune -print0 2>/dev/null)

# Agents sometimes pass a throwaway --cache-dir (mktemp under ~/.cache or
# $TMPDIR) and never delete it. Treat any Zig-shaped cache dir there the same.
echo "==> Stray Zig cache dirs"
for base in "$user_cache" "${TMPDIR:-/tmp}" /tmp; do
	[ -d "$base" ] || continue
	while IFS= read -r -d '' dir; do
		[ "$dir" = "$zig_global_cache" ] && continue
		[ -d "$dir/o" ] && [ -d "$dir/h" ] || continue
		if reason="$(build_active_in "$dir")"; then
			note_skip "$dir" "active build, $reason"
			continue
		fi
		gc_dir "$dir"
	done < <(find "$base" -mindepth 1 -maxdepth 3 -type d \
		\( -name 'antfly-*' -o -name .zig-cache -o -name zig-local \) -prune -print0 2>/dev/null)
done

echo "==> Zig global cache ($zig_global_cache)"
# Keep p/ (fetched packages); only compiled output is disposable. This
# cache is shared by every worktree's builds, so a zig/maker process
# anywhere -- not just one whose cwd or --cache-dir is here -- blocks the
# whole category rather than just deferring individual subdirs.
if reason="$(shared_cache_busy zig)"; then
	note_skip "$zig_global_cache" "shared cache, $reason"
elif [ -d "$zig_global_cache" ]; then
	for sub in o h z tmp; do
		gc_dir "$zig_global_cache/$sub"
	done
fi

echo "==> Rust target dirs"
while IFS= read -r -d '' dir; do
	wt="$(owning_worktree "$dir")"
	if reason="$(build_active_in "${wt:-$dir}")"; then
		note_skip "$dir" "active build, $reason"
		continue
	fi
	if have cargo-sweep; then
		echo "  cargo sweep --time $GC_MAX_AGE_DAYS: $(dirname "$dir")"
		run cargo sweep --time "$GC_MAX_AGE_DAYS" "$(dirname "$dir")" >/dev/null
	fi
	gc_dir "$dir"
done < <(find "$repo_root" \
	\( -name node_modules -o -name .git -o -name .zig-cache \) -prune -o \
	-type d -name target -path '*/rs/target' -prune -print0 2>/dev/null)
# Cargo trims ~/.cargo/registry and git checkouts automatically (1.88+).

echo "==> Go build cache"
# GOCACHE is one directory shared by every worktree's `go build`/`go test`.
if reason="$(shared_cache_busy go)"; then
	note_skip "go build cache" "shared cache, $reason"
elif have go; then
	echo "  go clean -cache ($(go env GOCACHE))"
	run go clean -cache
fi

echo "==> Python (uv) cache"
if reason="$(shared_cache_busy uv)"; then
	note_skip "uv cache" "shared cache, $reason"
elif have uv; then
	echo "  uv cache prune ($(uv cache dir 2>/dev/null))"
	run uv cache prune --quiet
fi

echo "==> Node caches"
if reason="$(shared_cache_busy node)"; then
	note_skip "pnpm/npm cache" "shared cache, $reason"
else
	if have pnpm; then
		echo "  pnpm store prune"
		run pnpm store prune >/dev/null
	fi
	if have npm; then
		echo "  npm cache verify"
		run npm cache verify >/dev/null
	fi
fi

if [ -n "$skip_log" ]; then
	echo
	echo "==> Skipped (active builds)"
	printf '%s\n' "$skip_log"
fi

echo
if [ "$DRY_RUN" = 1 ]; then
	echo "Dry run: nothing deleted."
else
	echo "Removed $(human "$freed_kb") of Zig/Rust build output; $(human "$(free_kb)") free."
fi
