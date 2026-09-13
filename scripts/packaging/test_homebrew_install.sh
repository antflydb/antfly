#!/usr/bin/env bash
# Exercise native relocation, C embedding, and migration from the old version 64.
# Run on a disposable Apple Silicon runner or with an isolated Homebrew prefix.
set -euo pipefail

formula_source=${1:?usage: test_homebrew_install.sh FORMULA ARCHIVE_DIR VERSION}
archive_dir=${2:?missing archive directory}
version=${3:?missing version}
test "$(uname -s)" = Darwin
test "$(uname -m)" = arm64
export HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_ANALYTICS=1
export HOMEBREW_NO_INSTALL_CLEANUP=1 HOMEBREW_NO_INSTALLED_DEPENDENTS_CHECK=1
if brew list --formula --versions antfly >/dev/null 2>&1; then
  echo "Refusing to replace an existing Antfly installation; use an isolated Homebrew prefix" >&2
  exit 1
fi

tap=antfly-ci/packaging-test
brew tap-new "$tap"
formula="$(brew --repository "$tap")/Formula/antfly.rb"
python3 - "$formula_source" "$archive_dir" "$formula" <<'PY'
import re
import sys
from pathlib import Path

source, archives, destination = map(Path, sys.argv[1:])
text = re.sub(
    r'https://releases\.antfly\.io/antfly/[^/]+/([^"\n]+)',
    lambda match: (archives.resolve() / match[1]).as_uri(),
    source.read_text(),
)
destination.write_text(text)
PY
cp "$formula" "$formula.fixed"
package="$tap/antfly"
if brew command trust >/dev/null 2>&1; then
  brew trust "$tap"
fi
installed_version() {
  basename "$(cd "$(brew --prefix "$package")" && pwd -P)"
}

# A clean installation must exit successfully and load the installed dylib.
brew install --formula "$package"
test "$(installed_version)" = "$version"
brew test "$package"
brew uninstall --formula "$package"

# Reproduce the old keg metadata with the same payload, then exercise upgrade.
python3 - "$formula" <<'PY'
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
text = re.sub(r'^  version ".*"$', '  version "64"', path.read_text(), flags=re.M)
text = text.replace('  version_scheme 1\n', '')
path.write_text(text)
PY
brew install --formula "$package"
test "$(installed_version)" = 64
cp "$formula.fixed" "$formula"
brew upgrade --formula "$package"
test "$(installed_version)" = "$version"
brew test "$package"
echo "Homebrew clean install, C embedding, and 64 -> $version upgrade passed"
