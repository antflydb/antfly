# App updates and releases

Antfly Dictation uses Sparkle 2 for signed, independent macOS app updates.
Official Apple Silicon apps include a pinned, released Antfly runtime.
Sparkle replaces that runtime with the app. Antfly is never compiled as part of the app build. User-selected external
servers and downloaded models have independent lifecycles.

## Installation and everyday updates

After the first signed app release is published, download its Apple Silicon ZIP
from `https://releases.antfly.io/antfly-dictation/releases/<version>/Antfly-Dictation-<version>-arm64.zip`
(or the optional GitHub mirror), quit
any development copy, and move **Antfly Dictation.app** into `/Applications`
(or your user account's `~/Applications`). Launch that installed copy. The first
migration from an ad hoc development build to Developer ID signing may require
re-enabling Accessibility for the installed app. Test permission continuity when
qualifying subsequent releases; macOS owns those permissions.

Use **Check for Updates…** in the application menu, menu-bar panel, or Settings.
Settings also provides automatic checks, automatic downloads, and beta opt-in.
Automatic checks and downloads are initially off. Sparkle owns the update
preferences; the beta preference is local to this Mac and is not included in
Antfly settings snapshots or preset JSON.

The stable and beta feeds contain signed metadata and EdDSA-signed archives.
Stable releases also advance the beta feed when newer than its current release.
Switching back to stable does not downgrade an installed beta; wait for a newer
stable release or manually install the desired release after backing up data.

An update restart waits for recording, processing, history operations, shortcut
recording, and permission setup to finish. It then prevents new work, restores
an owned temporary clipboard, and drains local history/audio writes through the
normal shutdown path. Pending remote uploads stay in the durable outbox.
If installation fails or is canceled, a pending installation handler is discarded;
local drains finish before the current app resumes and re-registers its shortcut.
Ordinary user-initiated Quit still cancels active dictation as before.

App bundles do not contain user data. Application Support and Keychain paths,
the bundle identifier `dev.antfly.dictation`, and the release signing team remain
stable. Presets, dictionary, settings, retained history, pending deletions, and
credentials survive replacement of the bundle. A future incompatible storage
change must introduce a tested migration before its release; the updater does
not silently reset storage or promise compatibility with older downgraded apps.

Development builds, including local release-optimization builds, have updating
explicitly disabled. Official release metadata and installation in Applications
are both required. A worktree app cannot replace itself with a public release.

## One-time organization setup

An organization Apple Developer membership is sufficient. An account holder or
administrator should create/download:

1. A **Developer ID Application** certificate and its private key, exported as a
   password-protected `.p12`. This is not an Apple Development or Mac App Store
   certificate. Keep its organization Team ID and full certificate common name.
2. An **App Store Connect API key** authorized to notarize apps for that team,
   including its `.p8` private key, Key ID, and organization Issuer ID.

Keep these files outside the repository. Never paste the private keys or export
password into an issue, PR, chat, shell command, or source file. The `.p8` download
is available only once; retain a secure backup. Back up the Sparkle signing key
securely as well. Losing that key can require manual installation or a deliberate
key rotation; do not generate a new key for every release.

The setup helper prompts for the `.p12` password and streams credentials directly
into GitHub Actions secrets. It checks the existing repository public key before
changing signing credentials. Routine setup requires this Mac's Keychain key to
match that identity; a new Mac must restore the original private key. It never
replaces an existing repository public key. First setup generates or reuses the
`antflydb-dictation` Keychain key and reserves the public identity with a
create-only API before uploading credentials. If a feed or any dictation release
already exists but the public-key variable is missing, setup stops for recovery.
It prints only the public Sparkle key. The temporary private-key export has
restrictive permissions, is checked against the deployed public key immediately
before upload, and is removed afterward.
Run from a checkout containing this change, authenticated with `gh` as a repo
administrator:

```sh
python3 examples/dictation/release/setup-github.py \
  --certificate /secure/path/DeveloperIDApplication.p12 \
  --api-key /secure/path/AuthKey_KEYID.p8 \
  --api-key-id KEYID \
  --issuer-id ORGANIZATION-ISSUER-UUID \
  --team-id TEAMID1234 \
  --identity 'Developer ID Application: Your Organization (TEAMID1234)'
```

On a replacement Mac, add `--sparkle-key /secure/path/original-sparkle-seed` to
the command above. The helper verifies that backup against the deployed public
key before importing it. Repeating setup with the same key is safe. If this Mac
has a conflicting `antflydb-dictation` Keychain entry, back it up and remove that
specific conflicting entry before restoring the original; the helper does not
overwrite it. Never delete the organization's only copy of a signing key.

A lost repository public-key variable must be restored from a trusted existing
signed release before rerunning setup. An API permission failure does not count
as first-time initialization. A genuine key rotation needs a separately reviewed
transition and compatibility plan; the helper has no force-rotation flag. With
verification before extraction enabled, Sparkle requires a Developer ID signed
DMG to rotate the EdDSA key. These ZIP updates cannot silently rotate it.

To configure only the GitHub approval boundary, without any credentials:

```sh
python3 examples/dictation/release/setup-github.py --environment-only
```

The `dictation-release` environment requires approval from the configuring
administrator and allows deployment only from the repository's default branch.
The helper does not replace an existing organization's approval policy or widen
its allowed branches. Signing and publishing are separate jobs behind that
boundary. Reviewers can be adjusted in GitHub environment settings.

Configuration inventory:

| Location | Name | Contents |
| --- | --- | --- |
| Repository variable | `DICTATION_SPARKLE_PUBLIC_KEY` | Public 32-byte EdDSA key, available to the unprivileged builder |
| Environment variable | `DICTATION_SIGNING_IDENTITY` | Full Developer ID Application certificate common name |
| Environment variable | `DICTATION_APPLE_TEAM_ID` | Organization Team ID |
| Environment secret | `DICTATION_CERTIFICATE_P12_BASE64` | Base64-encoded certificate and private-key export |
| Environment secret | `DICTATION_CERTIFICATE_PASSWORD` | Export password |
| Environment secret | `DICTATION_APPLE_API_KEY_P8` | App Store Connect API private key text |
| Environment secret | `DICTATION_APPLE_API_KEY_ID` | API Key ID |
| Environment secret | `DICTATION_APPLE_API_ISSUER_ID` | Organization Issuer ID |
| Environment secret | `DICTATION_SPARKLE_PRIVATE_KEY` | Modern base64-encoded 32-byte private seed |

## Cutting a release

Merge the updater and both release workflows to the default branch first.
Release only a reviewed commit on that branch. Tags are independent of server
releases: `dictation/vX.Y.Z`, `dictation/vX.Y.Z-beta.N`, or
`dictation/vX.Y.Z-rc.N`. Prerelease counters are 1–255. Version components obey
Apple bundle-version limits. Beta and RC tags publish to the beta feed; stable
tags publish to stable and advance beta only if newer.

```sh
git fetch origin main
git tag -a dictation/v0.1.0-beta.1 origin/main -m 'Antfly Dictation 0.1.0-beta.1'
git push origin refs/tags/dictation/v0.1.0-beta.1
```

The read-only tag workflow emits an untrusted request. A controller loaded from
the default branch checks the actual tag event, repository, exact tag commit,
and source ancestry. A read-only macOS job tests and builds an Apple Silicon bundle with locked Sparkle dependencies. No signing credential is exposed
to source builds or their tests.

After environment approval, a clean signing job uses controller-owned scripts to
validate the unsigned archive and embedded identity, sign nested Sparkle helpers
and the app with Hardened Runtime, notarize, staple, and verify Gatekeeper
acceptance. The microphone entitlement is included; Accessibility continues to
use the user's normal macOS grant. The private certificate is imported into a
temporary keychain, and the temporary keychain/API files are removed on exit.

The stapled bundle is archived and EdDSA-signed. The appcast itself is also signed,
and the app requires signed feed metadata and verification before extraction.
The Sparkle release tools are downloaded by exact version and SHA-256 from
`release/policy.json`; the Swift package is pinned in `Package.resolved`.

A separate publication job uses the existing **release-promotion** environment
and `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_ENDPOINT_URL` secrets.
The canonical bucket is **antfly-releases**. Apple credentials remain isolated
in **dictation-release**. Python storage dependencies come from the repository's
hash-locked `scripts/release/requirements.lock`.

All app objects live under `antfly-dictation/`, outside the server's `antfly/`
namespace. The app reuses the server release infrastructure's S3 immutable
publisher, verified recovery reader, schema-2 `artifacts.json` ledger validator,
and conditional-write transport. It has its own stable/beta product policy;
server npm/container/Homebrew promotions do not apply to a macOS app.

Publication first reserves a create-only signing identity at
`signing/public-key.json` and a create-only version identity at
`identities/<version>.json`. Changed keys or different builds of the same version
fail instead of replacing deployed bytes. Every signed member is stored both at
`artifacts/sha256/<digest>/<name>` and `releases/<version>/<name>`.
The exact member set is checked before uploading `artifacts.json` last as the
immutable seal, and checked again before publishing a channel.

Feed URLs:

- `https://releases.antfly.io/antfly-dictation/channels/stable/appcast.xml`
- `https://releases.antfly.io/antfly-dictation/channels/beta/appcast.xml`

Archive URL:
`https://releases.antfly.io/antfly-dictation/releases/<version>/Antfly-Dictation-<version>-arm64.zip`

The signed XML feed is the channel's atomic commit point. Conditional R2 writes
use the previous ETag (or create-only on first publication), with up to three
reconciliation attempts. Feeds use `Cache-Control: no-cache, no-transform`; do
not configure a CDN rule that overrides this or transforms the signed bytes.
Older releases cannot downgrade a channel; different signed bytes at the same
version are an error. Stable advances stable and beta independently. If only one
write succeeds, retry completes the other without replacing immutable assets.
A lost response is resolved by reading the exact existing bytes on retry.

Set repository variable `DICTATION_GITHUB_MIRROR=true` to enable a separate
GitHub mirror job **after** canonical R2 publication. Mirror failures cannot block
R2 feeds. Draft lookup includes the paginated releases listing because GitHub's
by-tag endpoint excludes drafts. Reconciliation reads assets by numeric release
ID, preserves verified uploads, and removes only empty matching draft placeholders.
GitHub does not own the app channel or Antfly's Latest release marker.

If signing succeeded but publication failed, use **Re-run failed jobs** to reuse
the signed artifact. Never re-sign an already reserved version: timestamps can
change its bytes. Once the R2 prefix is sealed, **Dictation release recovery**
can restore it using the exact tag, source commit, and trusted SHA-256 of its
`artifacts.json`. Recovery runs only from the default branch, under
release-promotion. It stages the downloads, checks the exact member set, hashes,
original identity, and established signing key, then finishes channel promotion
without signing credentials or rebuilding. Equivalent local CLI:

```sh
python3 examples/dictation/release/release.py recover \
  --tag dictation/v0.1.0-beta.1 --commit SOURCE_COMMIT \
  --ledger-sha256 TRUSTED_LEDGER_SHA256 \
  --out-dir dist/recovered --identity-output dist/identity.json
DEFAULT_BRANCH=main python3 examples/dictation/release/release.py publish \
  dist/identity.json dist/recovered
```

The ledger digest is printed during publication. Record it with the release run;
recovery requires a trusted digest, not one supplied by an untrusted mirror.
Unsealed prefixes cannot be recovered through this workflow; retry the original
signed artifact instead. Existing sealed app versions are retained indefinitely.
Server release garbage collection and aliases never touch the app namespace.
No production object is created by running the setup helper.

Qualify the first real signed release on a Mac: install in Applications, grant
microphone/Accessibility, dictate into another app, upgrade to a second signed
beta, verify preserved settings and permissions, and check that updating while
busy waits for dictation. This cannot be replaced by ad hoc signing tests.

## Local validation without production credentials

```sh
bash examples/dictation/release/fetch-tools.sh
python3 -m unittest discover -s examples/dictation/release -p 'test_*.py'
examples/dictation/test.sh
APP_OUTPUT_DIR="$PWD/examples/dictation/.build/validation-app" \
  examples/dictation/build-app.sh
codesign --verify --deep --strict \
  'examples/dictation/.build/validation-app/Antfly Dictation.app'
```

The native signing regression uses random synthetic keys via stdin to verify
archive and feed signatures and reject tampered payloads. It never accesses the
organization's signing key. Non-macOS policy tests skip that native regression.

## Managed runtime for downloaded apps

New installations default to **On this Mac (bundled Antfly)**. Existing settings
without `connectionMode` retain their external server; users can switch explicitly.
The external server field and credentials remain available and are preserved
when switching. Remote settings replacement cannot change the connection mode.

The unsigned build downloads the pinned **released** ARM64 Antfly archive from
`releases.antfly.io/antfly/<tag>/`. `release/policy.json` pins its tag, independent
source commit, ledger hash, archive hash and size, and extracted payload hash.
The fetcher checks the immutable release ledger before extraction, rejects unsafe
archive paths and links, and verifies the staged payload, including licenses.
Cached downloads are verified before reuse. It never follows a mutable latest URL
at app startup and never invokes Zig or builds Antfly from source.

The current pin is **v0.3.0-rc.13**, the published next-channel release used for
this app's compatibility qualification. The app and runtime require macOS 15 and
Apple Silicon. Intel app builds and runtime fallbacks are not supported.
The signed app contains the released CLI and its notices, not a separately built
engine. Before signing, the controller verifies the exact released payload;
Developer ID signing changes only the runtime's code signature. The normal hosted
macOS runner is sufficient: no internal Mac runner group or server toolchain setup
is required. Build-time execution checks happen before signing credentials are
available; the signing job only inspects the unsigned artifact as data.

Updating the pin requires a reviewed policy change and compatibility smoke tests.
The pinned release includes the native Apple provider bridge. Local mode offers
Whisper and native Apple speech and cleanup. Automatic local presets choose
Apple speech only when the server reports it ready, otherwise Whisper. Choosing
a transcriber explicitly preserves that choice across subsequent discovery. Apple transcription and generation
require macOS 26 or newer and appropriate speech assets or Apple Intelligence
readiness; macOS 15 users can use Whisper. The bridge comes from the verified
release archive, never from a separately source-built sidecar.

The app launches a headless copy of itself as a supervisor. An exclusive lock
protects the runtime data directory. Its Antfly child binds only to
127.0.0.1 on ports 47831 and 47832. Occupied ports block startup; the app never
attaches to or kills an unrelated server. A private pipe ties the supervisor's
lifetime to the app: EOF on quit, update, or app crash terminates the owned child,
with a ten-second grace period before killing its isolated process group, including
any owned inference workers. It never signals a shared process group.
Unexpected exits get at most three retries with exponential backoff. Startup
has a one-minute readiness deadline, and Settings provides status, retry, and
bounded diagnostics. Startup does not imply a transcription model is ready.

A random per-install administrator password stays in Keychain and travels to
the supervisor through the private pipe. Antfly's initial auth bootstrap reads it
from its environment; it never appears in arguments, configuration, or logs.
Public inference and table APIs require authentication. Missing credentials for
existing data cause a recoverable error; the app does not reset the database.
The supervised environment excludes developer DYLD overrides and provider keys.

Whisper models and engine data live in `~/Library/Application Support/AntflyDictation/Runtime`,
outside the app. **Download Whisper Tiny…** explicitly downloads the transcription
model and restarts the engine to discover it. Downloads have their own supervisor, an explicit Cancel model setup action,
and stop when the app exits. Native Apple providers are available only when supplied by the selected runtime
or external server. The current managed pin selects recognition without cleanup. Users can choose a discovered cleanup generator explicitly. An existing
server configuration is never edited or upgraded by this flow.

App updates wait for model setup through the normal working-state gate, stop the
supervisor before replacement, and restart it if Sparkle installation is canceled
or fails. Runtime data, models, credentials, and presets survive app replacement.
For local development without a bundled engine, choose External Antfly server.
Use `BUNDLE_RUNTIME=1 examples/dictation/build-app.sh` to exercise
the released-runtime path. No Antfly compiler or Apple bridge build is required.

References: [Sparkle setup](https://sparkle-project.org/documentation/),
[manual nested signing](https://sparkle-project.org/documentation/sandboxing/),
[Apple notarization](https://developer.apple.com/documentation/security/notarizing-macos-software-before-distribution).
