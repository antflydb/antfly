#!/usr/bin/env bash
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
mkdir -p "$destination/lib" "$destination/completions"
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
  printf 'glibc 2.28\n'
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
    case "$2" in */bash-completion/completions/antfly) true ;; *) false ;; esac; then
  printf 'injected completion activation failure\n' >&2
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
  CURL_ARGUMENTS_FILE="$arguments_file" \
    TEST_CHECKSUM="$checksum" \
    TEST_ACTUAL_CHECKSUM="${TEST_ACTUAL_CHECKSUM:-$checksum}" \
    TEST_FAIL_ACTIVATION="${TEST_FAIL_ACTIVATION:-}" \
    TEST_FAIL_GNU_DOWNLOAD="${TEST_FAIL_GNU_DOWNLOAD:-}" \
    ANTFLY_DOWNLOAD_CLASS="$class" \
    ANTFLY_LIBC="$libc" \
    HOME="$test_root/home" \
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

assert_activation_failure_rolls_back() {
  printf 'previous binary\n' >"$test_root/home/.local/bin/antfly"
  chmod +x "$test_root/home/.local/bin/antfly"
  printf 'previous library\n' >"$test_root/home/.local/lib/antfly/libantfly.so"
  printf 'previous completion\n' >"$test_root/home/.local/share/bash-completion/completions/antfly"

  local arguments_file="$test_root/curl-activation-failure.txt"
  local stderr_file="$test_root/stderr-activation-failure.txt"
  if TEST_FAIL_ACTIVATION=1 run_installer external "$arguments_file" "$stderr_file"; then
    echo "installer unexpectedly survived an injected activation failure" >&2
    exit 1
  fi
  grep -Fq 'restoring the previous Antfly installation' "$stderr_file"
  grep -Fq 'previous binary' "$test_root/home/.local/bin/antfly"
  grep -Fq 'previous library' "$test_root/home/.local/lib/antfly/libantfly.so"
  grep -Fq 'previous completion' "$test_root/home/.local/share/bash-completion/completions/antfly"
}

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
assert_activation_failure_rolls_back
assert_checksum_failure_preserves_install

publish_workflow="$repo_root/.github/workflows/cli-publish.yml"
for expected in \
  '--max-redirs 0' \
  'X-Antfly-Download-Channel: release-automation' \
  'X-Antfly-Audience: ci'
do
  grep -Fq -- "$expected" "$publish_workflow" || {
    echo "missing release automation marker '$expected' in $publish_workflow" >&2
    exit 1
  }
done

echo "install download, checksum, and transaction tests passed"
