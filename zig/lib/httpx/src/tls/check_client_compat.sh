#!/bin/sh
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0.
set -eu

root_dir=$(CDPATH='' cd -- "$(dirname -- "$0")/../../../.." && pwd)
std_dir=$(zig env | sed -n 's/^[[:space:]]*\.std_dir = "\([^"]*\)",$/\1/p')
upstream="$std_dir/crypto/tls/Client.zig"
compat="$root_dir/lib/httpx/src/tls/client_compat.zig"

hash_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

expected_upstream=21de8df98f17fc8ed00c03b54b60801fc168074c0e56f1d79d6d1b358096cfc5
actual_upstream=$(hash_file "$upstream")
if [ "$actual_upstream" != "$expected_upstream" ]; then
    echo "Zig TLS upstream drift: expected $expected_upstream, found $actual_upstream" >&2
    exit 1
fi

# Hash the two source files independently instead of hashing unified diff
# output, whose formatting differs between BSD and GNU diff implementations.
# With the upstream source pinned above, this also pins the exact patch.
expected_compat=8ef851bba7690eeaf549ef6b61d8cb6b38fd5091ea714b59757ea958b86237ba
actual_compat=$(hash_file "$compat")
if [ "$actual_compat" != "$expected_compat" ]; then
    echo "Zig TLS compatibility patch drift: expected compatibility source $expected_compat, found $actual_compat" >&2
    diff -u \
        --label zig-0.16.0/std/crypto/tls/Client.zig \
        --label lib/httpx/src/tls/client_compat.zig \
        "$upstream" "$compat" >&2 || true
    exit 1
fi
