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

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
mkdir -p "$test_root/bin" "$test_root/home"

checksum="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

cat >"$test_root/bin/uname" <<'EOF'
#!/bin/sh
case "$1" in
  -s) printf 'Linux\n' ;;
  -m) printf 'x86_64\n' ;;
  *) exit 1 ;;
esac
EOF

cat >"$test_root/bin/id" <<'EOF'
#!/bin/sh
if [ "${1:-}" = "-u" ]; then
  printf '1000\n'
else
  /usr/bin/id "$@"
fi
EOF

cat >"$test_root/bin/tar" <<'EOF'
#!/bin/sh
destination=''
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-C" ]; then
    shift
    destination="$1"
    break
  fi
  shift
done
[ -n "$destination" ] || exit 1
mkdir -p "$destination/lib/pkgconfig" "$destination/completions" \
  "$destination/include" "$destination/share/antfly/antfarm/assets"
cp "$TEST_PKGCONFIG_TEMPLATE" "$destination/lib/pkgconfig/libantfly.pc"
printf 'header\n' >"$destination/include/antfly.h"
printf 'dashboard\n' >"$destination/share/antfly/antfarm/index.html"
printf 'dashboard bundle\n' >"$destination/share/antfly/antfarm/assets/app.js"
printf '#!/bin/sh\nprintf "antfly test binary\\n"\n' >"$destination/antfly"
printf 'library\n' >"$destination/lib/libantfly.so"
printf 'completion\n' >"$destination/completions/antfly.bash"
EOF

cat >"$test_root/bin/curl" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >>"$CURL_ARGUMENTS_FILE"
output=''
url=''
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    shift
    output="$1"
  fi
  case "$1" in
    https://*) url="$1" ;;
  esac
  shift
done
case "$url" in
  */antfly_zig_checksums.txt)
    printf '%s  antfly_1.2.3_Linux_x86_64.tar.gz\n' "$TEST_CHECKSUM" >"$output"
    printf '%s  antfly_1.2.3_Linux_x86_64_gnu.tar.gz\n' "$TEST_CHECKSUM" >>"$output"
    ;;
  *_gnu.tar.gz)
    if [ "${TEST_FAIL_GNU_DOWNLOAD:-}" = "1" ]; then
      exit 22
    fi
    : >"$output"
    ;;
  *)
    : >"$output"
    ;;
esac
EOF

cat >"$test_root/bin/getconf" <<'EOF'
#!/bin/sh
if [ "${1:-}" = "GNU_LIBC_VERSION" ]; then
  if [ "${TEST_GLIBC_VERSION:-2.28}" = "unavailable" ]; then
    exit 1
  fi
  printf 'glibc %s\n' "${TEST_GLIBC_VERSION:-2.28}"
  exit 0
fi
exit 1
EOF

cat >"$test_root/bin/sha256sum" <<'EOF'
#!/bin/sh
printf '%s  %s\n' "${TEST_ACTUAL_CHECKSUM:-$TEST_CHECKSUM}" "$1"
EOF

cat >"$test_root/bin/sudo" <<'EOF'
#!/bin/sh
printf 'sudo must not be called for a clean user install\n' >&2
exit 99
EOF

cat >"$test_root/bin/mv" <<'EOF'
#!/bin/sh
if [ "${TEST_FAIL_ACTIVATION:-}" = "1" ] &&
    [ "$#" -eq 2 ] &&
    case "$1" in *.antfly-new.*) true ;; *) false ;; esac &&
    case "${TEST_FAIL_TARGET:-completion}:$2" in
      header:*/include/antfly.h|assets:*/share/antfly/antfarm|completion:*/bash-completion/completions/antfly) true ;;
      *) false ;;
    esac; then
  printf '%s:%s\n' "$TEST_FAIL_TARGET" "$2" >"$TEST_FAILURE_MARKER"
  printf 'injected activation failure\n' >&2
  exit 98
fi
exec /bin/mv "$@"
EOF
chmod +x "$test_root/bin/"*

run_installer() {
  local class="$1"
  local arguments_file="$2"
  local stderr_file="$3"
  local libc="${4:-auto}"
  local glibc_version="${5:-2.28}"
  local install_home="${TEST_INSTALL_HOME:-$test_root/home}"
  CURL_ARGUMENTS_FILE="$arguments_file" \
    TEST_CHECKSUM="$checksum" \
    TEST_PKGCONFIG_TEMPLATE="$repo_root/zig/pkg/antfly-embedded/libantfly.pc.in" \
    TEST_FAIL_TARGET="${TEST_FAIL_TARGET:-completion}" \
    TEST_FAILURE_MARKER="${TEST_FAILURE_MARKER:-$test_root/unexpected-failure}" \
    TEST_ACTUAL_CHECKSUM="${TEST_ACTUAL_CHECKSUM:-$checksum}" \
    TEST_FAIL_ACTIVATION="${TEST_FAIL_ACTIVATION:-}" \
    TEST_FAIL_GNU_DOWNLOAD="${TEST_FAIL_GNU_DOWNLOAD:-}" \
    TEST_GLIBC_VERSION="$glibc_version" \
    ANTFLY_DOWNLOAD_CLASS="$class" \
    ANTFLY_LIBC="$libc" \
    HOME="$install_home" \
    XDG_DATA_HOME="$install_home/.local/share" \
    XDG_CONFIG_HOME="$install_home/.config" \
    BASH_COMPLETION_USER_DIR="$install_home/.local/share/bash-completion" \
    ZSH_COMPLETION_USER_DIR="$install_home/.local/share/zsh/site-functions" \
    PATH="$test_root/bin:/usr/bin:/bin" \
    sh "$repo_root/scripts/install.sh" v1.2.3 >/dev/null 2>"$stderr_file"
}

run_case() {
  local class="$1"
  local libc="${2:-auto}"
  local arguments_file="$test_root/curl-${class:-missing}-${libc}.txt"
  local stderr_file="$test_root/stderr-${class:-missing}-${libc}.txt"
  run_installer "$class" "$arguments_file" "$stderr_file" "$libc"
  printf '%s\n' "$arguments_file" "$stderr_file"
}

assert_line() {
  local expected="$1"
  local file="$2"
  grep -Fqx -- "$expected" "$file" || {
    echo "missing expected curl argument '$expected' in $file" >&2
    exit 1
  }
}

assert_no_audience_header() {
  local file="$1"
  if grep -Fq -- "X-Antfly-Audience:" "$file"; then
    echo "unexpected audience header in $file" >&2
    exit 1
  fi
}

assert_installed_payload() {
  test -x "$test_root/home/.local/bin/antfly"
  grep -Fq 'library' "$test_root/home/.local/lib/antfly/libantfly.so"
  grep -Fq 'completion' "$test_root/home/.local/share/bash-completion/completions/antfly"
  grep -Fxq 'header' "$test_root/home/.local/include/antfly.h"
  grep -Fxq 'dashboard' "$test_root/home/.local/share/antfly/antfarm/index.html"
  grep -Fxq 'dashboard bundle' "$test_root/home/.local/share/antfly/antfarm/assets/app.js"
  test ! -e "$test_root/home/.local/share/antfly/antfarm/stale.js"
  grep -Fxq 'unrelated version' "$test_root/home/.local/share/antfly/versions/keep"

  local metadata="$test_root/home/.local/lib/antfly/pkgconfig/libantfly.pc"
  # Compare all metadata, including the unchanged include and rpath expressions.
  # shellcheck disable=SC2016 # pkg-config expands these variables, not the shell.
  sed -e 's|^prefix=.*|prefix=${pcfiledir}/../../..|' \
      -e 's|^libdir=.*|libdir=${prefix}/lib/antfly|' \
      "$repo_root/zig/pkg/antfly-embedded/libantfly.pc.in" >"$test_root/expected.pc"
  cmp "$test_root/expected.pc" "$metadata"
}

assert_checksum_failure_preserves_install() {
  printf 'previous binary\n' >"$test_root/home/.local/bin/antfly"
  chmod +x "$test_root/home/.local/bin/antfly"
  printf 'previous library\n' >"$test_root/home/.local/lib/antfly/libantfly.so"

  local arguments_file="$test_root/curl-checksum-failure.txt"
  local stderr_file="$test_root/stderr-checksum-failure.txt"
  if TEST_ACTUAL_CHECKSUM="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" \
      run_installer external "$arguments_file" "$stderr_file"; then
    echo "installer unexpectedly accepted a checksum mismatch" >&2
    exit 1
  fi
  grep -Fq 'Checksum verification failed' "$stderr_file"
  grep -Fq 'previous binary' "$test_root/home/.local/bin/antfly"
  grep -Fq 'previous library' "$test_root/home/.local/lib/antfly/libantfly.so"
}

assert_no_installer_paths() {
  local install_home="$1"
  # Capture the complete result so pipefail cannot hide matches behind SIGPIPE.
  local leftovers
  leftovers="$(find "$install_home" -name '*.antfly-*' -print)"
  if [ -n "$leftovers" ]; then
    printf 'installer left staging or backup paths:\n%s\n' "$leftovers" >&2
    exit 1
  fi
}

assert_activation_failure_rolls_back() {
  local target="$1"
  local mode="$2"
  local install_home="$test_root/home"
  if [ "$mode" = fresh ]; then
    # Each failure starts with its own empty prefix; earlier cases cannot seed it.
    install_home="$test_root/fresh-$target"
    mkdir -p "$install_home"
  else
    printf 'previous binary\n' >"$install_home/.local/bin/antfly"
    chmod +x "$install_home/.local/bin/antfly"
    printf 'previous library\n' >"$install_home/.local/lib/antfly/libantfly.so"
    printf 'previous completion\n' >"$install_home/.local/share/bash-completion/completions/antfly"
    printf 'previous header\n' >"$install_home/.local/include/antfly.h"
    printf 'previous dashboard\n' >"$install_home/.local/share/antfly/antfarm/index.html"
    printf 'previous bundle\n' >"$install_home/.local/share/antfly/antfarm/assets/app.js"
    printf 'previous metadata\n' >"$install_home/.local/lib/antfly/pkgconfig/libantfly.pc"
    printf 'stale asset\n' >"$install_home/.local/share/antfly/antfarm/stale.js"
  fi

  local expected_target
  case "$target" in
    header) expected_target="$install_home/.local/include/antfly.h" ;;
    assets) expected_target="$install_home/.local/share/antfly/antfarm" ;;
    completion) expected_target="$install_home/.local/share/bash-completion/completions/antfly" ;;
    *) echo "unknown failure target: $target" >&2; exit 1 ;;
  esac
  local arguments_file="$test_root/curl-activation-$mode-$target.txt"
  local stderr_file="$test_root/stderr-activation-$mode-$target.txt"
  local marker="$test_root/failure-$mode-$target.txt"
  local exit_code
  if TEST_FAIL_ACTIVATION=1 TEST_FAIL_TARGET="$target" \
      TEST_FAILURE_MARKER="$marker" TEST_INSTALL_HOME="$install_home" \
      run_installer external "$arguments_file" "$stderr_file"; then
    echo "installer unexpectedly survived $mode $target activation failure" >&2
    exit 1
  else
    exit_code=$?
  fi
  if [ "$exit_code" -ne 98 ]; then
    printf 'expected injected failure status 98, got %s (%s %s)\n' \
      "$exit_code" "$mode" "$target" >&2
    cat "$stderr_file" >&2
    exit 1
  fi
  # Confirm the intended rename was reached, rather than accepting an earlier error.
  if ! grep -Fxq "$target:$expected_target" "$marker"; then
    printf 'expected activation failure at %s (%s)\n' "$expected_target" "$mode" >&2
    cat "$stderr_file" >&2
    exit 1
  fi
  grep -Fq 'restoring the previous Antfly installation' "$stderr_file"
  assert_no_installer_paths "$install_home"

  if [ "$mode" = fresh ]; then
    local remaining_files
    remaining_files="$(find "$install_home" \( -type f -o -type l \) -print)"
    if [ -n "$remaining_files" ]; then
      printf 'failed fresh install left payload files:\n%s\n' "$remaining_files" >&2
      exit 1
    fi
  else
    grep -Fxq 'previous binary' "$install_home/.local/bin/antfly"
    grep -Fxq 'previous library' "$install_home/.local/lib/antfly/libantfly.so"
    grep -Fxq 'previous completion' "$install_home/.local/share/bash-completion/completions/antfly"
    grep -Fxq 'previous header' "$install_home/.local/include/antfly.h"
    grep -Fxq 'previous dashboard' "$install_home/.local/share/antfly/antfarm/index.html"
    grep -Fxq 'previous bundle' "$install_home/.local/share/antfly/antfarm/assets/app.js"
    grep -Fxq 'previous metadata' "$install_home/.local/lib/antfly/pkgconfig/libantfly.pc"
    grep -Fxq 'stale asset' "$install_home/.local/share/antfly/antfarm/stale.js"
    grep -Fxq 'unrelated version' "$install_home/.local/share/antfly/versions/keep"
  fi
}

# An upgrade must replace its dashboard leaf while preserving sibling runtimes.
mkdir -p "$test_root/home/.local/share/antfly/antfarm" \
  "$test_root/home/.local/share/antfly/versions"
printf 'stale asset\n' >"$test_root/home/.local/share/antfly/antfarm/stale.js"
printf 'unrelated version\n' >"$test_root/home/.local/share/antfly/versions/keep"

employee_files="$(run_case employee)"
employee_arguments="$(printf '%s\n' "$employee_files" | sed -n '1p')"
assert_line "-A" "$employee_arguments"
assert_line "antfly-installer/1" "$employee_arguments"
assert_line "X-Antfly-Audience: employee" "$employee_arguments"
assert_line "--max-redirs" "$employee_arguments"
assert_line "0" "$employee_arguments"

ci_files="$(run_case ci)"
ci_arguments="$(printf '%s\n' "$ci_files" | sed -n '1p')"
assert_line "X-Antfly-Audience: ci" "$ci_arguments"
assert_line "--max-redirs" "$ci_arguments"
assert_line "0" "$ci_arguments"

external_files="$(run_case external)"
external_arguments="$(printf '%s\n' "$external_files" | sed -n '1p')"
assert_line "antfly-installer/1" "$external_arguments"
assert_no_audience_header "$external_arguments"
assert_line "https://releases.antfly.io/antfly/v1.2.3/antfly_1.2.3_Linux_x86_64_gnu.tar.gz" "$external_arguments"

for old_glibc in 2.17 2.27; do
  old_glibc_arguments="$test_root/curl-glibc-${old_glibc}.txt"
  old_glibc_stderr="$test_root/stderr-glibc-${old_glibc}.txt"
  run_installer external "$old_glibc_arguments" "$old_glibc_stderr" auto "$old_glibc"
  assert_line "https://releases.antfly.io/antfly/v1.2.3/antfly_1.2.3_Linux_x86_64.tar.gz" "$old_glibc_arguments"
  grep -Fq "older than the supported GNU archive floor (2.28)" "$old_glibc_stderr"
done

unknown_libc_arguments="$test_root/curl-unknown-libc.txt"
unknown_libc_stderr="$test_root/stderr-unknown-libc.txt"
run_installer external "$unknown_libc_arguments" "$unknown_libc_stderr" auto unavailable
assert_line "https://releases.antfly.io/antfly/v1.2.3/antfly_1.2.3_Linux_x86_64.tar.gz" "$unknown_libc_arguments"
grep -Fq "Could not detect the Linux libc" "$unknown_libc_stderr"

forced_gnu_arguments="$test_root/curl-forced-gnu-old-glibc.txt"
forced_gnu_stderr="$test_root/stderr-forced-gnu-old-glibc.txt"
run_installer external "$forced_gnu_arguments" "$forced_gnu_stderr" gnu 2.17
assert_line "https://releases.antfly.io/antfly/v1.2.3/antfly_1.2.3_Linux_x86_64_gnu.tar.gz" "$forced_gnu_arguments"

musl_files="$(run_case external musl)"
musl_arguments="$(printf '%s\n' "$musl_files" | sed -n '1p')"
assert_line "https://releases.antfly.io/antfly/v1.2.3/antfly_1.2.3_Linux_x86_64.tar.gz" "$musl_arguments"

fallback_arguments="$test_root/curl-fallback.txt"
fallback_stderr="$test_root/stderr-fallback.txt"
TEST_FAIL_GNU_DOWNLOAD=1 run_installer external "$fallback_arguments" "$fallback_stderr"
assert_line "https://releases.antfly.io/antfly/v1.2.3/antfly_1.2.3_Linux_x86_64_gnu.tar.gz" "$fallback_arguments"
assert_line "https://releases.antfly.io/antfly/v1.2.3/antfly_1.2.3_Linux_x86_64.tar.gz" "$fallback_arguments"
grep -Fq "falling back to the portable musl build" "$fallback_stderr"

invalid_files="$(run_case not-valid)"
invalid_arguments="$(printf '%s\n' "$invalid_files" | sed -n '1p')"
invalid_stderr="$(printf '%s\n' "$invalid_files" | sed -n '2p')"
assert_no_audience_header "$invalid_arguments"
grep -Fq "Ignoring invalid ANTFLY_DOWNLOAD_CLASS" "$invalid_stderr"

assert_installed_payload
assert_no_installer_paths "$test_root/home"
for target in header assets completion; do
  assert_activation_failure_rolls_back "$target" upgrade
  assert_activation_failure_rolls_back "$target" fresh
done
assert_checksum_failure_preserves_install

echo "install download, checksum, and transaction tests passed"
