#!/bin/sh
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

# Install a verified, extracted standalone grep release under a user prefix.
set -eu
prefix=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix)
      [ "$#" -ge 2 ] || { echo "--prefix requires a directory" >&2; exit 2; }
      [ -n "$2" ] || { echo "--prefix requires a nonempty directory" >&2; exit 2; }
      prefix=$2
      shift 2
      ;;
    --help) echo "usage: ./install.sh [--prefix DIRECTORY]"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [ -z "$prefix" ]; then prefix=${HOME:?HOME must be set}/.local; fi
case "$prefix" in
  /*) ;;
  *) echo "--prefix must be an absolute directory" >&2; exit 2 ;;
esac
source_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd -P)
[ -f "$source_dir/antfly-grep" ] || { echo "missing standalone antfly-grep binary" >&2; exit 1; }
mkdir -p "$prefix/bin" "$prefix/share/antfly-grep" "$prefix/share/man/man1" \
  "$prefix/share/bash-completion/completions" "$prefix/share/zsh/site-functions" \
  "$prefix/share/fish/vendor_completions.d"
install -m 755 "$source_dir/antfly-grep" "$prefix/bin/antfly-grep"
cp -R "$source_dir/share/antfly-grep/." "$prefix/share/antfly-grep/"
install -m 644 "$source_dir/share/man/man1/antfly-grep.1" "$prefix/share/man/man1/antfly-grep.1"
install -m 644 "$source_dir/completions/antfly-grep.bash" "$prefix/share/bash-completion/completions/antfly-grep"
install -m 644 "$source_dir/completions/antfly-grep.zsh" "$prefix/share/zsh/site-functions/_antfly-grep"
install -m 644 "$source_dir/completions/antfly-grep.fish" "$prefix/share/fish/vendor_completions.d/antfly-grep.fish"
install -m 644 "$source_dir/LICENSE" "$prefix/share/antfly-grep/LICENSE"
install -m 644 "$source_dir/THIRD_PARTY_NOTICES.md" "$prefix/share/antfly-grep/THIRD_PARTY_NOTICES.md"
cp -R "$source_dir/LICENSES" "$prefix/share/antfly-grep/"
printf 'Installed antfly-grep to %s/bin/antfly-grep\n' "$prefix"
