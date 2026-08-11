#!/bin/sh
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0.
set -eu

root_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../../../.." && pwd)
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

expected_upstream=1464bd0d53f799e7714fa073a0783cb94c68c10fb1ab4381246e01f9cb130c8a
actual_upstream=$(hash_file "$upstream")
if [ "$actual_upstream" != "$expected_upstream" ]; then
    echo "Zig TLS upstream drift: expected $expected_upstream, found $actual_upstream" >&2
    exit 1
fi

diff_file=$(mktemp)
trap 'rm -f "$diff_file"' EXIT HUP INT TERM
diff -u \
    --label zig-0.16.0/std/crypto/tls/Client.zig \
    --label lib/httpx/src/tls/client_compat.zig \
    "$upstream" "$compat" >"$diff_file" || status=$?
if [ "${status:-0}" -ne 1 ]; then
    echo "unable to produce the expected Zig TLS compatibility diff" >&2
    exit 1
fi

expected_diff=b66fe1e58e6007fc33e058742795e69b6531c61c8c4c789a3e8e97b4380cdfd8
actual_diff=$(hash_file "$diff_file")
if [ "$actual_diff" != "$expected_diff" ]; then
    echo "Zig TLS compatibility patch drift: expected $expected_diff, found $actual_diff" >&2
    exit 1
fi
