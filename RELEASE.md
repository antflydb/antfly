# Release Design

This repo releases the native Zig runtime, Go fallback runtime, CLI installer
packages, container image, and SDKs from tags. The intended long-term shape is
that the Zig runtime is built once per supported native platform and every
downstream channel consumes those same archives.

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

Linux archives are built with musl on native Linux runners. macOS arm64 is built
on a macOS runner with Metal enabled. We do not cross-compile the Zig runtime in
GoReleaser because ReleaseFast cross-compiles have repeatedly failed under CI
memory pressure, especially Linux arm64 from amd64.

## Pipeline Ownership

`.github/workflows/antfly-release.yml` is the tag release pipeline.

1. `build-zig-runtime-archives` builds the native Zig archives on native runners
   and uploads them as GitHub Actions artifacts.
2. `goreleaser` publishes the Go fallback runtime and release metadata, then
   uploads the prebuilt Zig archives to GitHub Releases and R2 as extra files.
3. `package-cli-artifacts` builds the `antfly-cli` wheels and `@antfly/cli` npm
   packages from the same Zig archives.
4. `publish-cli-pypi` and `publish-cli-npm` publish the CLI installer packages
   with trusted publishing/provenance.
5. `publish-zig-homebrew` updates the stable `antfly` Homebrew formula from the
   Zig archive checksums. RC tags do not update the stable tap formula.
6. `publish-container` calls `.github/workflows/antfly-container.yml` with
   `artifact_source: github`, so the container image uses the Linux archives
   already built by the release.

`.github/workflows/antfly-container.yml` still supports standalone container
publishes. In standalone mode it builds the Linux archives on native Linux
runners, uploads them to the container artifact bucket, and packages images from
those tarballs. In release mode it skips the redundant build and uploads the
already-built release archives to the same bucket path expected by Cloud Build.

GoReleaser OSS does not have the prebuilt-artifact feature needed to model
externally built binaries as first-class build outputs. For now, GoReleaser owns
the Go fallback artifacts and release metadata, while prebuilt Zig archives are
attached via `release.extra_files` and `blobs.extra_files`.

## Version Behavior

Stable tags use `vX.Y.Z`; RC tags use `vX.Y.Z-rc.N`.

Stable releases publish:

- GitHub Release artifacts
- R2 release artifacts
- `latest` R2 channel artifacts
- Go fallback Homebrew formula `antfly-go`
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

## Preflight

`.github/workflows/antfly-release-preflight.yml` builds the same native Zig
archive matrix used by the tag release. It exists to catch runner, Zig, Metal,
musl, and archive-layout failures before recreating a release tag.
