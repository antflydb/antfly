#!/bin/bash
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
umask 077
cd "$(dirname "$0")/.."
identity_file="$1"
unsigned="$2"
: "${DICTATION_CERTIFICATE_P12_BASE64:?Missing Developer ID certificate}"
: "${DICTATION_CERTIFICATE_PASSWORD:?Missing certificate password}"
: "${DICTATION_APPLE_API_KEY_P8:?Missing App Store Connect API private key}"
: "${DICTATION_APPLE_API_KEY_ID:?Missing API key ID}"
: "${DICTATION_APPLE_API_ISSUER_ID:?Missing organization API issuer ID}"
: "${DICTATION_SPARKLE_PRIVATE_KEY:?Missing Sparkle private seed}"
: "${SPARKLE_PUBLIC_KEY:?Missing Sparkle public key}"
: "${APPLE_TEAM_ID:?Missing organization team ID}"
: "${SIGNING_IDENTITY:?Missing Developer ID Application identity}"
[[ "$SIGNING_IDENTITY" == 'Developer ID Application: '* ]]
mkdir -p .build
secrets_dir="$(mktemp -d "$PWD/.build/release-secrets.XXXXXX")"
keychain="$secrets_dir/signing.keychain-db"
cleanup() {
  security delete-keychain "$keychain" >/dev/null 2>&1 || true
  rm -rf "$secrets_dir"
}
trap cleanup EXIT
python3 - "$secrets_dir" <<'PY'
import base64, os, subprocess, sys
from pathlib import Path
p=Path(sys.argv[1])
p.joinpath('certificate.p12').write_bytes(base64.b64decode(os.environ['DICTATION_CERTIFICATE_P12_BASE64'],validate=True))
p.joinpath('AuthKey.p8').write_text(os.environ['DICTATION_APPLE_API_KEY_P8'])
seed=base64.b64decode(os.environ['DICTATION_SPARKLE_PRIVATE_KEY'].strip(),validate=True)
if len(seed)!=32: raise ValueError('Use a modern 32-byte Sparkle private seed')
public=subprocess.check_output(['swift','-module-cache-path','.build/modules','release/public-key.swift'],
 input=base64.b64encode(seed)+b'\n').decode().strip()
if public!=os.environ['SPARKLE_PUBLIC_KEY']:
    raise ValueError('Sparkle private key does not match the embedded public key')
PY
password="$(openssl rand -hex 32)"
security create-keychain -p "$password" "$keychain"
security set-keychain-settings -lut 21600 "$keychain"
security unlock-keychain -p "$password" "$keychain"
security import "$secrets_dir/certificate.p12" -k "$keychain" -P "$DICTATION_CERTIFICATE_PASSWORD" -T /usr/bin/codesign >/dev/null
security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$password" "$keychain" >/dev/null
security list-keychains -d user -s "$keychain" login.keychain-db
python3 release/release.py verify-zip "$unsigned"
mkdir -p .build/signed dist/release
# The unsigned artifact is data; none of its source scripts are executed.
ditto -x -k "$unsigned" .build/signed
app="$PWD/.build/signed/Antfly Dictation.app"
python3 release/release.py verify-bundle "$identity_file" "$app" --key "$SPARKLE_PUBLIC_KEY"
bash release/sign-app.sh "$app"
codesign -dv --verbose=4 "$app" 2> "$secrets_dir/signature.txt"
grep -Fx "TeamIdentifier=$APPLE_TEAM_ID" "$secrets_dir/signature.txt" >/dev/null
notary_zip="$secrets_dir/notarize.zip"
ditto -c -k --sequesterRsrc --keepParent "$app" "$notary_zip"
xcrun notarytool submit "$notary_zip" --key "$secrets_dir/AuthKey.p8" \
  --key-id "$DICTATION_APPLE_API_KEY_ID" --issuer "$DICTATION_APPLE_API_ISSUER_ID" --wait --timeout 30m
xcrun stapler staple "$app"
xcrun stapler validate "$app"
codesign --verify --deep --strict "$app"
spctl --assess --type execute --verbose "$app"
version="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "$identity_file")"
archive="$PWD/dist/release/Antfly-Dictation-$version-universal.zip"
ditto -c -k --sequesterRsrc --keepParent "$app" "$archive"
bash release/fetch-tools.sh
# Pass private key material through stdin, never a command argument or output.
signature="$(python3 - "$archive" <<'PY'
import os,subprocess,sys
print(subprocess.check_output(['.build/updater-tools/bin/sign_update','--ed-key-file','-','-p',sys.argv[1]],
 input=os.environ['DICTATION_SPARKLE_PRIVATE_KEY'].strip().encode(),text=False).decode().strip())
PY
)"
python3 release/release.py appcast "$identity_file" "$archive" "$signature" > dist/release/appcast.xml
python3 - <<'PY'
import os,subprocess
subprocess.run(['.build/updater-tools/bin/sign_update','--ed-key-file','-','dist/release/appcast.xml'],
 input=os.environ['DICTATION_SPARKLE_PRIVATE_KEY'].strip().encode(),check=True)
PY
(cd dist/release && shasum -a 256 ./*.zip appcast.xml > SHA256SUMS)
python3 release/release.py manifest "$identity_file" dist/release
