# App updates and releases

Antfly Dictation uses Sparkle 2 for signed, independent macOS app updates.
The Antfly server and its models have their own lifecycle. Updating the app does
not update a server, download models, or change the configured connection.

## Installation and everyday updates

Download the universal ZIP from an official `dictation/v…` GitHub Release, quit
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

The setup helper prompts for the `.p12` password, generates or reuses the
`antflydb-dictation` Sparkle key in your login Keychain, and streams credentials
directly into GitHub Actions secrets. It prints only the public Sparkle key.
The temporary private-key export has restrictive permissions and is removed.
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
and source ancestry. A read-only macOS job tests and builds a universal ARM64 /
Intel bundle with locked Sparkle dependencies. No signing credential is exposed
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

A separate publication job consumes the exact signed artifact, rechecks the tag
and current controller policy, verifies payload digests, and creates/verifies
immutable GitHub Release assets. It never overwrites an existing asset with
different bytes and never takes the repository-wide Latest marker from Antfly.
Only after the downloads are published does it atomically fast-forward the
`dictation-updates` feed branch. Feed publication refuses version regression,
conflicting bytes at the same version, or concurrent lost updates.

Feed URLs:

- `https://raw.githubusercontent.com/antflydb/antfly/dictation-updates/stable/appcast.xml`
- `https://raw.githubusercontent.com/antflydb/antfly/dictation-updates/beta/appcast.xml`

These URLs become available after the first corresponding release. No release
or feed is created just by running the setup helper.

If publication fails after signing, use **Re-run failed jobs** on that run, so
publication reuses the signed artifact. Do not re-run signing to replace an
already-published version: notarization timestamps can produce different bytes.
Do not move release tags. A correction is a new version. If the controller policy
changes while a release is queued, release from a new tag containing the reviewed
policy instead of bypassing its identity checks.

Qualify the first real signed release on a Mac: install in Applications, grant
microphone/Accessibility, dictate into another app, upgrade to a second signed
beta, verify preserved settings and permissions, and check that updating while
busy waits for dictation. This cannot be replaced by ad hoc signing tests.

## Local validation without production credentials

```sh
bash examples/dictation/release/fetch-tools.sh
python3 -m unittest discover -s examples/dictation/release -p 'test_*.py'
examples/dictation/test.sh
UNIVERSAL_BUILD=1 APP_OUTPUT_DIR="$PWD/examples/dictation/.build/validation-app" \
  examples/dictation/build-app.sh
codesign --verify --deep --strict \
  'examples/dictation/.build/validation-app/Antfly Dictation.app'
```

The native signing regression uses random synthetic keys via stdin to verify
archive and feed signatures and reject tampered payloads. It never accesses the
organization's signing key. Non-macOS policy tests skip that native regression.

## Should the app also distribute Antfly?

Offer an **optional managed local runtime download** in a future local-server
setup flow. Keep the desktop ZIP and server releases independent. Do not bundle
model weights or silently upgrade an existing server.

The managed-runtime design should verify pinned release provenance and hashes,
install versioned binaries under Application Support, bind only to loopback,
use a Keychain-backed per-install credential, and own only its own process. Keep
an existing working runtime until a replacement passes health/capability checks.
Models need explicit size, download, and storage controls. A user-supplied local
or remote server must remain an equally supported choice.

Current Antfly macOS runtime releases are ARM64. Intel Macs can run this universal
app against a remote server; an Intel managed runtime needs its own qualified
server build before being offered. This update implementation does not yet
install, launch, update, or bundle an Antfly server. Existing model discovery and
processing capability checks continue to determine what the configured server
can do.

References: [Sparkle setup](https://sparkle-project.org/documentation/),
[manual nested signing](https://sparkle-project.org/documentation/sandboxing/),
[Apple notarization](https://developer.apple.com/documentation/security/notarizing-macos-software-before-distribution).
