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

`.github/workflows/antfly-release.yml` is the tag release pipeline.

1. `build-zig-runtime-archives` builds the canonical Zig archives on the
   appropriate native or cross-compilation runners and uploads them as GitHub
   Actions artifacts.
2. `package-cli-artifacts` calls `.github/workflows/cli-package.yml` to build the
   `antfly-cli` wheels and `@antfly/cli` npm packages directly from the native
   Actions artifacts. It creates one source-commit-bound manifest for those
   exact bytes.
3. `publish-release-assets` combines the native archives and CLI snapshot into
   one deterministic artifact ledger, attests every payload file, and stores the
   exact bytes on the draft GitHub Release and under immutable, content-addressed
   object-storage keys. Top-level trusted-publisher jobs promote those same CLI
   bytes. A manual dispatch verifies the saved snapshot against its tag, commit,
   digest, and release-workflow signer before promoting it without rebuilding.
4. `publish-zig-homebrew` updates the stable `antfly` Homebrew formula from the
   Zig archive checksums. RC tags do not update the stable tap formula.
5. `publish-container` calls `.github/workflows/antfly-container.yml` with
   `artifact_source: github`, so the container image uses the Linux archives
   already built by the release.

After the unified release payload is published, package registry publishes,
Homebrew, and container publishing fan out independently. A PyPI or npm publish
failure must not block the container image for the same tag. npm platform
packages publish before the top-level selector, and existing npm or PyPI files
are skipped only when their registry digest matches, so a partial publication
is safe to retry without hiding content drift. Existing GitHub release assets
are likewise accepted only when byte-identical; release automation never
replaces a saved artifact.

`.github/workflows/antfly-container.yml` still supports standalone container
publishes. In standalone mode it builds the GNU Linux archives on native Linux
runners, uploads them to the container artifact bucket, and packages images from
those tarballs. In release mode it skips the redundant build and uploads the
already-built GNU release archives to the same bucket path expected by Cloud
Build. Cloud Build requires an explicit immutable artifact URI; the config does
not carry a mutable or ABI-ambiguous default artifact.

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
