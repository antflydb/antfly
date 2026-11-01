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

# Detect live native build processes and *where* they are working, so
# cache_gc.sh can skip exactly the worktree (or shared cache) a build is
# using instead of refusing to clean the whole machine. A 30-minute mtime
# window (see gc_dir in cache_gc.sh) catches most cases, but a 1.5-2 hour
# release build can go that long without writing near the top of its own
# cache dir, so we also look at what is actually running.
#
# Zig builds show up under two process names: "zig" itself (including its
# compile-step and test-runner children, which are also comm "zig"), and
# "maker" -- zig compiles build.zig into a build-runner binary cached
# under a content hash and execs it, so the top-level driver's comm is
# whatever that cached binary happens to be named, observed in production
# as:
#   .../global/o/<hash>/maker build --cache-dir <dir> --prefix <dir> ...
# Rust builds are cargo plus its backends (rustc, clippy-driver,
# rust-lld). Go, uv, and the Node package managers (pnpm/npm, themselves
# node scripts) only matter here for the single shared cache each one
# uses; nothing in cache_gc.sh cleans a per-worktree Go/uv/Node cache, so
# they are not consulted for the per-worktree checks below.
#
# Process-name lists are overridable so a test can stand in a real but
# differently-named process (e.g. `sleep`) for "a build is running"
# without needing a fake binary that fools `pgrep -x`.
ZIG_BUILD_PROC_NAMES="${CACHE_GC_ZIG_PROC_NAMES:-zig maker}"
RUST_BUILD_PROC_NAMES="${CACHE_GC_RUST_PROC_NAMES:-cargo rustc clippy-driver rust-lld}"
GO_BUILD_PROC_NAMES="${CACHE_GC_GO_PROC_NAMES:-go}"
UV_BUILD_PROC_NAMES="${CACHE_GC_UV_PROC_NAMES:-uv}"

_activity_have() { command -v "$1" >/dev/null 2>&1; }

# True once, cheaply, if we have no way to inspect processes at all. Every
# caller below treats "can't tell" the same as "a build is active": it is
# far cheaper to skip a GC pass than to delete a live build's cache.
_inspection_available() { _activity_have pgrep && _activity_have ps; }

# PIDs (one per line) of processes whose comm exactly matches one of the
# space-separated names in $1. A single pgrep call per name-set, per the
# AGENTS.md/CLAUDE.md guidance to stay fast and not shell out to
# `lsof +D` or scan every process's cwd.
_pids_for_names() {
	local pattern
	pattern="$(printf '%s' "$1" | tr -s ' ' '|')"
	[ -n "$pattern" ] || return 0
	pgrep -x "$pattern" 2>/dev/null
}

# npm and pnpm are themselves node scripts and show up to pgrep as a
# plain "node" process; a bare "node" is otherwise far too common on a
# dev machine (editors, agents, Electron apps) to treat as a build, so
# require the command line to actually mention the package manager.
_node_build_pids() {
	local pid cmd
	_pids_for_names node | while IFS= read -r pid; do
		cmd="$(ps -o command= -p "$pid" 2>/dev/null)"
		case "$cmd" in
		*pnpm* | *npm*)
			printf '%s\n' "$pid"
			;;
		esac
	done
	_pids_for_names "pnpm npm"
}

# Working directory of a pid, macOS (lsof) or Linux (/proc) alike.
_proc_cwd() {
	local pid="$1"
	if [ -r "/proc/$pid/cwd" ]; then
		readlink "/proc/$pid/cwd" 2>/dev/null
	elif _activity_have lsof; then
		lsof -a -d cwd -p "$pid" -Fn 2>/dev/null | sed -n 's/^n//p' | tail -n1
	fi
}

_proc_cmdline() { ps -o command= -p "$1" 2>/dev/null; }

_proc_comm() { ps -o comm= -p "$1" 2>/dev/null | sed 's/^ *//;s/ *$//'; }

# Paths following --cache-dir, --prefix, --global-cache-dir, or
# --global-cache in a command line (space- or =-separated): the flags zig
# build (and build-runner wrappers that mimic it) use to point a build at
# a cache outside its worktree's default .zig-cache/zig-out. Losing exotic
# quoting here is fine; we only need a prefix match against a directory.
_cache_arg_paths() {
	# Intentional word-splitting: this is a command line, not one token.
	# shellcheck disable=SC2086
	set -- $1
	while [ $# -gt 0 ]; do
		case "$1" in
		--cache-dir=* | --prefix=* | --global-cache-dir=* | --global-cache=*)
			printf '%s\n' "${1#*=}"
			;;
		--cache-dir | --prefix | --global-cache-dir | --global-cache)
			[ $# -gt 1 ] && printf '%s\n' "$2"
			;;
		esac
		shift
	done
}

# True when $1 and $2 are the same path or one contains the other.
_paths_overlap() {
	local a="${1%/}" b="${2%/}"
	[ -n "$a" ] && [ -n "$b" ] || return 1
	[ "$a" = "$b" ] && return 0
	case "$a" in "$b"/*) return 0 ;; esac
	case "$b" in "$a"/*) return 0 ;; esac
	return 1
}

# Echoes one reason line and returns 0 if a process whose comm is in the
# space-separated name list $2 has a cwd, or a
# --cache-dir/--prefix/--global-cache(-dir) argument, overlapping path
# $1. Prints nothing and returns 1 otherwise.
_active_reason_for() {
	local path="$1" names="$2" pid cwd cmd arg
	if ! _inspection_available; then
		printf 'process inspection unavailable\n'
		return 0
	fi
	for pid in $(_pids_for_names "$names"); do
		cwd="$(_proc_cwd "$pid")"
		if _paths_overlap "$path" "$cwd"; then
			printf 'pid %s (%s) cwd %s\n' "$pid" "$(_proc_comm "$pid")" "$cwd"
			return 0
		fi
		cmd="$(_proc_cmdline "$pid")"
		while IFS= read -r arg; do
			[ -n "$arg" ] || continue
			if _paths_overlap "$path" "$arg"; then
				printf 'pid %s (%s) cache arg %s\n' "$pid" "$(_proc_comm "$pid")" "$arg"
				return 0
			fi
		done < <(_cache_arg_paths "$cmd")
	done
	return 1
}

# Is a zig or cargo/rustc-family build working in or under $1 (a worktree
# root, or a stray cache dir outside any worktree)? Echoes why on a match.
# Used to gate the per-worktree/per-dir caches cache_gc.sh cleans
# (.zig-cache, zig-out*, zig-local, rs/target, stray Zig cache dirs).
build_active_in() {
	_active_reason_for "$1" "$ZIG_BUILD_PROC_NAMES $RUST_BUILD_PROC_NAMES"
}

# Is the given shared/global cache category in use anywhere on the
# machine right now? These caches (Zig's global cache, Go's build cache,
# uv's cache, the pnpm store / npm cache) are not per-worktree, so a build
# in *any* worktree -- or one driven straight from a path outside
# .worktrees/ entirely -- can be reading them while they look idle by
# every per-directory mtime rule. Echoes a reason on a match.
shared_cache_busy() {
	local category="$1" pid
	if ! _inspection_available; then
		printf 'process inspection unavailable\n'
		return 0
	fi
	case "$category" in
	zig) pid="$(_pids_for_names "$ZIG_BUILD_PROC_NAMES" | head -n1)" ;;
	go) pid="$(_pids_for_names "$GO_BUILD_PROC_NAMES" | head -n1)" ;;
	uv) pid="$(_pids_for_names "$UV_BUILD_PROC_NAMES" | head -n1)" ;;
	node) pid="$(_node_build_pids | head -n1)" ;;
	*) return 1 ;;
	esac
	[ -n "$pid" ] || return 1
	printf 'pid %s (%s) running\n' "$pid" "$(_proc_comm "$pid")"
	return 0
}
