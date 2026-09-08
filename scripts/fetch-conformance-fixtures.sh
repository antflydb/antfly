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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
suite=all
dest=/tmp
while [[ $# -gt 0 ]]; do
  case "$1" in
    --suite|--dest)
      if [[ $# -lt 2 || -z "$2" ]]; then
        echo "$1 requires a value" >&2
        exit 2
      fi
      if [[ "$1" == --suite ]]; then suite="$2"; else dest="$2"; fi
      shift 2
      ;;
    --help|-h)
      echo "usage: $0 [--suite toon|image|audio|all] [--dest /absolute/fixture/directory]"
      exit 0
      ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done
case "$suite" in toon|image|audio|all) ;; *) echo "unknown suite: $suite" >&2; exit 2 ;; esac
if [[ "$dest" != /* ]]; then
  echo "--dest must be an absolute path (also pass it as -Dconformance-fixtures when testing)" >&2
  exit 2
fi
mkdir -p "$dest"
(cd "$ROOT/zig" && zig build conformance-tools)
tools_dir="$ROOT/zig/zig-out/bin"
if [[ "$suite" == toon || "$suite" == all ]]; then
  "$tools_dir/lib-toon-conformance" fetch "$dest/toon-format-spec"
fi
if [[ "$suite" == image || "$suite" == all ]]; then
  "$tools_dir/lib-image-conformance-fetch" fetch "$dest/openjpeg-data"
  "$tools_dir/image-jpeg-seed-corpora-e2e" fetch "$dest/libjpeg-turbo-seed-corpora"
fi
if [[ "$suite" == audio || "$suite" == all ]]; then
  "$tools_dir/lib-audio-xiph-conformance" fetch "$dest/audio-xiph-corpora"
  "$tools_dir/lib-audio-misc-conformance" fetch "$dest/audio-misc-corpora"
fi
