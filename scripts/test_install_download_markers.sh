#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
mkdir -p "$test_root/bin" "$test_root/home"

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
exit 0
EOF

cat >"$test_root/bin/curl" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$CURL_ARGUMENTS_FILE"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    shift
    : >"$1"
    break
  fi
  shift
done
EOF
chmod +x "$test_root/bin/"*

run_case() {
  local class="$1"
  local arguments_file="$test_root/curl-${class:-missing}.txt"
  local stderr_file="$test_root/stderr-${class:-missing}.txt"
  CURL_ARGUMENTS_FILE="$arguments_file" \
    ANTFLY_DOWNLOAD_CLASS="$class" \
    HOME="$test_root/home" \
    PATH="$test_root/bin:/usr/bin:/bin" \
    sh "$repo_root/scripts/install.sh" v1.2.3 >/dev/null 2>"$stderr_file"
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

invalid_files="$(run_case not-valid)"
invalid_arguments="$(printf '%s\n' "$invalid_files" | sed -n '1p')"
invalid_stderr="$(printf '%s\n' "$invalid_files" | sed -n '2p')"
assert_no_audience_header "$invalid_arguments"
grep -Fq "Ignoring invalid ANTFLY_DOWNLOAD_CLASS" "$invalid_stderr"

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

echo "install download marker tests passed"
