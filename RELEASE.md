# Release Design

This repo releases the native Zig runtime, CLI installer packages, container
image, and SDKs from tags. The intended long-term shape is that each supported
OS, architecture, and libc ABI is built once and every downstream channel
consumes those same archives.

## Release Artifacts

The canonical Zig runtime artifacts are tarballs named:

- `antfly_<version>_Darwin_arm64.tar.gz`
- `antfly_<version>_Linux_arm64.tar.gz`
- `antfly_<version>_Linux_arm64_gnu.tar.gz`
- `antfly_<version>_Linux_x86_64.tar.gz`
- `antfly_<version>_Linux_x86_64_gnu.tar.gz`

Each archive has this root layout:

```text
antfly
completions/
share/
lib/
include/antfly.h
README.md
LICENSE
THIRD_PARTY_NOTICES.md
```

Linux has an explicit two-ABI contract:

- Unsuffixed archives are portable musl builds. They remain CPU-only and are
  used on musl, glibc older than 2.28, and unknown Linux libc environments.
- `_gnu` archives target glibc 2.28 and include the runtime-loaded CUDA and
  PJRT/XLA integrations. Containers, Linux Homebrew, and Python manylinux wheels
  consume these archives.

The npm CLI publishes only GNU Linux packages under the existing unsuffixed npm
package names. Its supported Node.js 24 runtime has the same glibc 2.28 floor as
the GNU archive. The standalone shell installer still checks the glibc version
and falls back to the portable musl archive when the GNU compatibility floor is
not met.

All release targets use `ReleaseFast`. Linux amd64 and GNU arm64 build on their
native Linux architectures; portable musl arm64 and macOS arm64 cross-compile
from the amd64 Linux runner. The macOS build uses the pinned cross-compilation
SDK and enables Metal and Accelerate.

## Pipeline Ownership

Release is split into an unprivileged artifact plane and a default-branch
promotion plane so code loaded from a tag never receives publication
credentials.

1. `.github/workflows/antfly-artifact-build.yml` is the sole reusable artifact
   builder. With only read permission, it builds the canonical Zig archives,
   calls `.github/workflows/cli-package.yml`, extracts `install.sh` and
   `openapi.yaml` directly from the selected Git commit, and uploads the native
   archives, CLI snapshot, commit-bound source snapshot, and
   source-commit-and-channel-bound release request as Actions artifacts. The
   tag-triggered `.github/workflows/antfly-release-build.yml`
   and manually triggered `.github/workflows/antfly-nightly.yml` are thin
   callers of this same builder.
2. Completion of either caller triggers `.github/workflows/antfly-release.yml` through
   `workflow_run`. GitHub loads this privileged promotion workflow from the
   default branch. It checks the channel identity and source commit, combines the build
   outputs into one schema-versioned artifact ledger, verifies it, attests
   every payload file, and stores the exact bytes under immutable,
   content-addressed object-storage keys. Stable and next additionally stage
   those bytes on a draft GitHub Release; nightly deliberately does not create
   a GitHub Release.
3. The promotion controller separates the complete payload into
   ledger, runtime, and CLI scopes, and verifies source ancestry, attestations,
   exact scope membership, and every digest.
4. npm, PyPI, the Homebrew formula, and the single multi-architecture container
   image consume those verified scopes. The container publisher is callable
   only by this controller and consumes the GNU runtime archives built in step
   1. It first builds run-scoped images, seals them under the release-ledger
   digest, and then compare-or-creates the version tags. A retry can reuse an
   identical image but cannot overwrite a released version with different
   bytes.
5. Mutable install channels are transactions. `scripts/release/channels.json`
   is the canonical policy for `stable`, `next`, and `nightly`: tag class,
   ordering rule, journal, package-registry eligibility, mutable aliases,
   GitHub visibility, and recovery source. Compare-and-swap journals in R2
   record each channel's `pending` and `current` release identity (tag, source
   commit, and ledger digest). A failed run must resume the same pending
   identity, and recovery cannot move a channel backward.

Channel bootstrap is fail-closed. The discovery controller treats only an
explicit missing release, package, or dist-tag as empty state; authentication,
rate-limit, malformed-response, and network failures stop promotion.

npm platform packages publish before the top-level selector, and existing npm
or PyPI files are skipped only when their registry digest matches, so a partial
publication is safe to retry without hiding content drift. Existing GitHub
release assets are likewise accepted only when byte-identical; release
automation never replaces a saved artifact. GitHub release visibility is also
one-way: a retry may add missing identical assets to a published release but
never returns it to draft state.

Normal registry promotion is triggered by `workflow_run`; the explicit
`promote-release-channel` `repository_dispatch` event is the general recovery
path (`promote-cli-release` remains as a compatible stable/next event name).
Both entry points load the promotion workflow from the default branch rather
than an operator-selected ref. The `pypi` and `npm` environments admit this CLI
promotion from `main`; typed tag rules preserve pre-transition `v*` releases
and the existing SDK publishers without allowing similarly named branches.
Both environments require review with self-approval disabled. Recovery
requests include the exact `artifacts.json` SHA-256; the promotion verifies
that digest, its attestation, tag and source commit before granting either
registry job access.

`.github/workflows/antfly-container.yml` has only a `workflow_call` entry point.
It accepts only the default-branch promotion controller's normal `workflow_run`
or recovery dispatch events, rechecks source ancestry and the release-ledger
digest, and consumes the verified GNU archives. Cloud Build uses an immutable
source-commit-and-ledger-addressed artifact URI and has no mutable or
ABI-ambiguous default input.

Temporary run-scoped container tags are build staging only. The
`sha256-<ledger digest>` and `<version>` tags are immutable publication
records; registry retention may remove staging tags after the release. There
is deliberately no automatic rollback entry point for mutable channels. A
rollback requires a separately reviewed administrative change to the channel
journal and aliases, rather than disguising an old-release recovery as a new
promotion.

The canonical ABI, archive, package, wheel, backend, and consumer matrix lives
in `scripts/release/platforms.json`; channel behavior lives in
`scripts/release/channels.json`. Release jobs, CLI packaging, Homebrew, and
promotion read those policies instead of maintaining independent conditionals
or platform tables. Python
dependencies used by the release control plane are exact and hash-locked in
`scripts/release/requirements.lock`; Node and npm versions are exact as well.

Release metadata and object-storage publishing are implemented as explicit
scripts under `scripts/release/`:

- `stage_release_source.py` extracts release support files from the exact Git
  commit and records their digests in `source-snapshot.json`.
- `build_release_payload.py` verifies that source snapshot before combining it
  with release archives and CLI packages, writes `antfly_zig_checksums.txt`,
  and generates `metadata.json` and `artifacts.json`.
- `create_github_release.py` creates or updates the draft GitHub Release,
  generates release notes through the GitHub API, and accepts existing assets
  only when their digest matches the local payload.
- `release_channels.py` validates and resolves the canonical channel policy and
  derives the npm, Python, and container identities from one accepted version.
  Stable and next use version precedence; nightly uses its monotonically
  increasing workflow-run sequence.
- `release_channel_state.py` compare-and-swaps each channel journal, prevents
  backward promotion, and makes an interrupted promotion resumable only by the
  same release identity.
- `download_objectstorage.py` restores a nightly's exact ledger members from
  immutable object storage for recovery; the normal ledger and attestation
  verification still runs before promotion.
- `publish_objectstorage.py` first writes content-addressed and versioned keys
  with compare-or-fail semantics, then updates mutable channel aliases only
  after every immutable upload succeeds. The release workflow currently uses
  the S3-compatible path for Cloudflare R2, but the script also has GCS and
  local modes for future storage backends and dry-run smoke tests.

## Version Behavior

Stable tags use `vX.Y.Z`; RC tags use `vX.Y.Z-rc.N`. Nightly snapshots use
`v0.0.0-dev.<GitHub run ID>`. A nightly is a channel, not a cadence: the
workflow is manual today, and a schedule or default-branch trigger can be added
later without changing the artifact or promotion model.

Release identity is deliberately narrower than generic SemVer: the core always
contains three components, prereleases use the supported dev/alpha/beta/RC
forms, and build metadata is rejected because it cannot be represented by every
publication target (notably container tags).

Run a snapshot for the current default-branch head with
`gh workflow run antfly-nightly.yml`. To reproduce a snapshot from a specific
default-branch commit, add `-f source_commit=<40-character-commit>`.

Stable releases publish:

- GitHub Release artifacts
- R2 release artifacts
- `latest` R2 channel artifacts
- Zig Homebrew formula `antfly`
- npm CLI packages with dist-tag `latest`
- PyPI CLI wheels
- container tags `<version>` and `latest`

RC releases publish:

- GitHub Release artifacts marked prerelease
- R2 release artifacts
- `next` R2 channel artifacts
- npm CLI packages with dist-tag `next`
- PyPI CLI wheels using PEP 440 prerelease versions, for example
  `0.2.0-rc.1` becomes `0.2.0rc1`
- container tags `<version>` and `next`

RC releases do not update the `latest` R2 channel, Homebrew stable formula, or
container `latest` tag.

Nightly snapshots publish:

- immutable R2 release artifacts and the `nightly` R2 channel
- npm CLI packages with dist-tag `nightly`
- container tags `<version>` and `nightly`

Nightlies do not publish to PyPI or Homebrew and do not create GitHub Releases.
Their immutable recovery source is R2.

Package registries are immutable. If an RC publish reaches npm or PyPI, the same
version cannot be republished after recreating the tag; cut the next RC instead.
