# CLI Packaging

Antfly has separate packages for SDKs and CLI installation:

- Python SDK: `antfly-sdk`
- TypeScript SDK: `@antfly/sdk`
- Go Antfly Lite binding: `github.com/antflydb/antfly/go/pkg/antflylite`
- Python CLI installer: `antfly-cli`
- npm CLI installer: `@antfly/cli`

The CLI packages do not build Antfly from source. They repack the native Zig
runtime archives produced by the main release workflow.

The native Zig runtime archives are also the distribution vehicle for Antfly
Lite's C ABI. Each archive contains:

```text
antfly
share/
lib/
include/antfly.h
README.md
LICENSE
```

`lib/` contains the platform-specific `libantfly` shared library. Language
bindings that embed Lite, including the Go `antflylite` binding, link against
that library and include `include/antfly.h`.

The Python, npm, and Homebrew CLI installer packages preserve the same Lite C
ABI files from the native archive. Consumers that need embedded Lite can install
one of those packages or unpack the native runtime archive, then point their
language binding at the packaged `libantfly` library. The Go `antflylite`
module carries a matching header copy for standalone builds, but the release
packages and archives also keep `include/antfly.h` available for direct C
consumers.

## Release Flow

1. Publish the normal Antfly release tag, for example `v0.2.0`.
2. The main release workflow uploads native archives under
   `https://releases.antfly.io/antfly/v0.2.0/`.
3. The release workflow downloads those archives, repacks them, and publishes
   `antfly-cli` and `@antfly/cli`.

The CLI workflow also supports manual runs with a version input and `cli/v*`
tags for republishing or recovery. Trusted publishing for the normal release
path should be configured against `.github/workflows/antfly-release.yml`.

Linux releases have two libc variants. The unsuffixed archive is the portable,
CPU-only musl build used by musl hosts and direct portable downloads. The
`_gnu` archive is the glibc build used for glibc hosts and runtime-loaded
integrations:

| Consumer | Linux archive | Reason |
| --- | --- | --- |
| `scripts/install.sh` | auto-detected | `_gnu` on glibc; musl on musl or unknown Linux |
| Direct portable archive downloads | musl (unsuffixed) | Portable standalone CLI |
| Python `manylinux` wheels | `_gnu` | glibc-compatible executable and C ABI library |
| npm Linux platform packages | libc-specific | GNU and musl packages preserve both supported Linux ABIs |
| Homebrew on Linux | `_gnu` | Linuxbrew runs on glibc and installs `libantfly.so` |
| Container images | `_gnu` | CUDA, PJRT/XLA, and other host plugins use the glibc ABI |

The release workflow publishes these native archives:

```text
antfly_0.2.0_Darwin_arm64.tar.gz
antfly_0.2.0_Linux_arm64.tar.gz
antfly_0.2.0_Linux_arm64_gnu.tar.gz
antfly_0.2.0_Linux_x86_64.tar.gz
antfly_0.2.0_Linux_x86_64_gnu.tar.gz
```

The packaging script consumes the Darwin archive and both variants for each
Linux architecture. Python wheels use only the GNU archives. npm publishes GNU
and musl platform packages and selects between them at runtime. The GNU targets
pin glibc 2.28 to match the wheel platform tag and keep that compatibility floor
stable across Zig upgrades. All archives include `include/antfly.h` and the
platform library under `lib/`;
`scripts/packaging/build_zig_release_archive.sh` builds the runtime and then the
`capi` target into the same archive prefix. GNU Linux archives compile in the
CUDA and PJRT/XLA backends, which discover their driver or plugin at runtime and
remain usable on CPU-only glibc hosts. PJRT/XLA use still requires a compatible
plugin supplied by the runtime environment.

The shell installer detects glibc with `getconf` and otherwise checks for musl.
Set `ANTFLY_LIBC=gnu` or `ANTFLY_LIBC=musl` to override detection, for example
when preparing an installation for a different host. In automatic mode it
falls back to musl when an older release does not provide a GNU archive.

For prerelease tags, npm uses the release version directly. Python wheels use
PEP 440 equivalents, for example `v0.2.0-dev10` becomes `0.2.0.dev10`.
Stable npm releases publish with the `latest` dist-tag. Prerelease npm versions
publish with the `next` dist-tag, so `npm install -g @antfly/cli` stays on the
latest stable release and `npm install -g @antfly/cli@next` can install RC/dev
builds.

It creates:

```text
dist/cli-packages/python/antfly_cli-0.2.0-py3-none-macosx_11_0_arm64.whl
dist/cli-packages/python/antfly_cli-0.2.0-py3-none-manylinux_2_28_aarch64.whl
dist/cli-packages/python/antfly_cli-0.2.0-py3-none-manylinux_2_28_x86_64.whl
```

and populates npm platform packages:

```text
@antfly/cli-darwin-arm64
@antfly/cli-linux-arm64
@antfly/cli-linux-arm64-gnu
@antfly/cli-linux-x64
@antfly/cli-linux-x64-gnu
```

The existing unsuffixed npm Linux packages remain musl packages for backwards
compatibility. The `-gnu` packages declare `libc: glibc`; the unsuffixed Linux
packages declare `libc: musl`. The top-level `@antfly/cli` package exposes the
`antfly` bin and selects the package matching the host OS, CPU, and libc.

Before the first automated release, bootstrap `@antfly/cli-linux-arm64-gnu` and
`@antfly/cli-linux-x64-gnu`, then configure npm trusted publishing against the
same release workflows as the existing platform packages.
