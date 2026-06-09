# Release Design

This repo releases the native Zig runtime, CLI installer packages, container
image, and SDKs from tags. The intended long-term shape is that the Zig runtime
is built once per supported native platform and every downstream channel
consumes those same archives.

## Release Artifacts

The canonical Zig runtime artifacts are tarballs named:

- `antfly_<version>_Darwin_arm64.tar.gz`
- `antfly_<version>_Linux_arm64.tar.gz`
- `antfly_<version>_Linux_x86_64.tar.gz`

Each archive has this root layout:

```text
antfly
share/
README.md
LICENSE
```

Linux archives are built with musl on native Linux runners. Linux amd64 uses
`ReleaseFast`; Linux arm64 currently uses `ReleaseSmall` because the full
ReleaseFast build hits a single-process LLVM allocation failure in CI even with
`-j1`. macOS arm64 is built on a macOS runner with Metal enabled. We do not
cross-compile the Zig runtime because ReleaseFast cross-compiles have repeatedly
failed under CI memory pressure, especially Linux arm64 from amd64.

## Pipeline Ownership

`.github/workflows/antfly-release.yml` is the tag release pipeline.

1. `build-zig-runtime-archives` builds the native Zig archives on native runners
   and uploads them as GitHub Actions artifacts.
2. `publish-release-assets` builds the release payload, creates or updates the
   draft GitHub Release, uploads the Zig archives and release metadata as GitHub
   Release assets, then publishes the payload to object storage.
3. `package-cli-artifacts` builds the `antfly-cli` wheels and `@antfly/cli` npm
   packages from the same Zig archives.
4. `publish-cli-pypi` and `publish-cli-npm` publish the CLI installer packages
   with trusted publishing/provenance.
5. `publish-zig-homebrew` updates the stable `antfly` Homebrew formula from the
   Zig archive checksums. RC tags do not update the stable tap formula.
6. `publish-container` calls `.github/workflows/antfly-container.yml` with
   `artifact_source: github`, so the container image uses the Linux archives
   already built by the release.

After the native archives and release payload are published, package registry
publishes, Homebrew, and container publishing fan out independently. A PyPI or
npm publish failure must not block the container image for the same tag.

`.github/workflows/antfly-container.yml` still supports standalone container
publishes. In standalone mode it builds the Linux archives on native Linux
runners, uploads them to the container artifact bucket, and packages images from
those tarballs. In release mode it skips the redundant build and uploads the
already-built release archives to the same bucket path expected by Cloud Build.

Release metadata and object-storage publishing are implemented as explicit
scripts under `scripts/release/`:

- `build_release_payload.py` copies release archives and support files into a
  payload directory, writes `antfly_zig_checksums.txt`, and generates
  `metadata.json` and `artifacts.json`.
- `create_github_release.py` creates or updates the draft GitHub Release,
  generates release notes through the GitHub API, and replaces matching release
  assets.
- `publish_objectstorage.py` uploads the payload to object storage. The release
  workflow currently uses the S3-compatible path for Cloudflare R2, but the
  script also has GCS and local modes for future storage backends and dry-run
  smoke tests.

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
