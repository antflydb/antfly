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

Release is deliberately split across two workflows so code loaded from an
untrusted tag never receives publication credentials.

1. `.github/workflows/antfly-release-build.yml` is the tag workflow. With only
   read permission, it builds the canonical Zig archives, calls
   `.github/workflows/cli-package.yml`, and uploads the native archives, CLI
   snapshot, and source-commit-bound release request as Actions artifacts.
2. Completion triggers `.github/workflows/antfly-release.yml` through
   `workflow_run`. GitHub loads this privileged promotion workflow from the
   default branch. It checks the tag and source commit, combines the build
   outputs into one schema-versioned artifact ledger, verifies it, attests
   every payload file, and stores the exact bytes on a draft GitHub Release and
   under immutable, content-addressed object-storage keys.
3. The promotion controller separates the complete payload into
   ledger, runtime, and CLI scopes, and verifies source ancestry, attestations,
   exact scope membership, and every digest.
4. npm, PyPI, Homebrew, and the single multi-architecture container image
   consume those verified scopes. The container publisher is callable only by
   this controller and consumes the GNU runtime archives built in step 1.
5. Stable container aliases move only after the registry and Homebrew versioned
   publications succeed. `promote-release-channel` then updates object-storage
   aliases and publishes the draft GitHub Release. RC releases skip stable
   aliases and Homebrew.

npm platform packages publish before the top-level selector, and existing npm
or PyPI files are skipped only when their registry digest matches, so a partial
publication is safe to retry without hiding content drift. Existing GitHub
release assets are likewise accepted only when byte-identical; release
automation never replaces a saved artifact.

Normal registry promotion is triggered by `workflow_run`; the explicit
`promote-cli-release` `repository_dispatch` event is only the recovery path.
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

The canonical ABI, archive, package, wheel, backend, and consumer matrix lives
in `scripts/release/platforms.json`. Release jobs, CLI packaging, and Homebrew
read that policy instead of maintaining independent platform tables. Python
dependencies used by the release control plane are exact and hash-locked in
`scripts/release/requirements.lock`; Node and npm versions are exact as well.

Release metadata and object-storage publishing are implemented as explicit
scripts under `scripts/release/`:

- `build_release_payload.py` copies release archives and support files into a
  payload directory, writes `antfly_zig_checksums.txt`, and generates
  `metadata.json` and `artifacts.json`.
- `create_github_release.py` creates or updates the draft GitHub Release,
  generates release notes through the GitHub API, and accepts existing assets
  only when their digest matches the local payload.
- `publish_objectstorage.py` first writes content-addressed and versioned keys
  with compare-or-fail semantics, then updates mutable channel aliases only
  after every immutable upload succeeds. The release workflow currently uses
  the S3-compatible path for Cloudflare R2, but the script also has GCS and
  local modes for future storage backends and dry-run smoke tests.

## Version Behavior

Stable tags use `vX.Y.Z`; RC tags use `vX.Y.Z-rc.N`.

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
- npm CLI packages with dist-tag `next`
- PyPI CLI wheels using PEP 440 prerelease versions, for example
  `0.2.0-rc.1` becomes `0.2.0rc1`
- container tag `<version>`

RC releases do not update the `latest` R2 channel, Homebrew stable formula, or
container `latest` tag.

Package registries are immutable. If an RC publish reaches npm or PyPI, the same
version cannot be republished after recreating the tag; cut the next RC instead.
