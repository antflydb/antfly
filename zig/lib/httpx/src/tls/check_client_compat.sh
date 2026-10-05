#!/bin/sh
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

expected_upstream=c6fbf80138bcf5cf5e46090294e94159815e2e175df56256679ddec87cf64d2c
actual_upstream=$(hash_file "$upstream")
if [ "$actual_upstream" != "$expected_upstream" ]; then
    echo "Zig TLS upstream drift: expected $expected_upstream, found $actual_upstream" >&2
    exit 1
fi

# Hash the two source files independently instead of hashing unified diff
# output, whose formatting differs between BSD and GNU diff implementations.
# With the upstream source pinned above, this also pins the exact patch.
expected_compat=23316976277d5960102376254bf34d871ea0866e50456ba41d865d8f71bcb3af
actual_compat=$(hash_file "$compat")
if [ "$actual_compat" != "$expected_compat" ]; then
    echo "Zig TLS compatibility patch drift: expected compatibility source $expected_compat, found $actual_compat" >&2
    diff -u \
        --label zig-0.17.0/std/crypto/tls/Client.zig \
        --label lib/httpx/src/tls/client_compat.zig \
        "$upstream" "$compat" >&2 || true
    exit 1
fi
