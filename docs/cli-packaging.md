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
2. The reusable CLI packaging workflow consumes the native GitHub Actions
   artifacts from that same run and builds all npm tarballs and Python wheels
   once.
3. The workflow records their hashes, source commit, npm version, and PEP 440
   Python version in `cli-snapshot.json`. The top-level workflow merges that
   snapshot with the native archives into a deterministic `artifacts.json`
   ledger and attests every exact payload file.
4. GitHub Release assets and versioned object-storage objects are immutable:
   retries accept an existing object only when its digest matches. Object
   storage also retains content-addressed objects under `sha256/<digest>/`.
5. The tag run sends the version and ledger digest through a
   `promote-cli-release` repository dispatch. GitHub always loads that promotion
   path from the default branch; top-level trusted-publisher jobs then promote
   the exact snapshot bytes to PyPI and npm. They skip an existing version only
   after comparing the registry digest, so retries cannot conceal drift. Native
   archives remain available under
   `https://releases.antfly.io/antfly/v0.2.0/` for direct installation.

For recovery, send the same repository dispatch with an existing release tag
and the SHA-256 of its `artifacts.json` asset:

```sh
gh api --method POST repos/antflydb/antfly/dispatches \
  -f event_type=promote-cli-release \
  -f 'client_payload[version]=v0.2.0' \
  -f 'client_payload[ledger_sha256]=<64-character-sha256>'
```

Recovery downloads the saved ledger, npm tarballs, wheels, and manifest from
the GitHub release; verifies every GitHub attestation against the expected
release-workflow signer, tag ref, repository, and source-commit digest; binds
the promotion to the supplied ledger digest; verifies every artifact hash; and
promotes those exact bytes. It never checks out historical or operator-selected
source to rebuild or publish registry artifacts.
The reusable `.github/workflows/cli-package.yml` workflow only builds the
original snapshot and cannot be dispatched directly; both trusted publication
jobs remain in the top-level release workflow. The `pypi` and `npm` GitHub
environments admit the CLI promotion only from the `main` branch, which is the
ref used by `repository_dispatch` runs. Typed `v*` tag rules preserve releases
made before this transition, and separate typed tag rules preserve the existing
Python and TypeScript SDK publishers. None of those tag patterns admits a
similarly named branch. Both environments require approval from a repository
administrator and prevent the triggering administrator from approving their
own deployment.

Linux releases have two libc variants. The unsuffixed archive is the portable,
CPU-only musl build used by musl hosts and direct portable downloads. The
`_gnu` archive is the glibc build used for glibc hosts and runtime-loaded
integrations:

| Consumer | Linux archive | Reason |
| --- | --- | --- |
| `scripts/install.sh` | auto-detected | `_gnu` on glibc 2.28+; musl on older glibc, musl, or unknown Linux |
| Direct portable archive downloads | musl (unsuffixed) | Portable standalone CLI |
| Python `manylinux` wheels | `_gnu` | glibc-compatible executable and C ABI library |
| npm Linux platform packages | `_gnu` | The supported Node.js 24 runtime and npm packages use glibc 2.28+ |
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

The packaging script consumes the Darwin archive and GNU archive for each Linux
architecture. Python wheels and npm packages both use the GNU archives. The
portable musl archives remain release assets for direct downloads and the shell
installer, but are not repackaged for Python or npm. The GNU targets pin glibc
2.28 to match the wheel platform tag and supported Node.js runtime, keeping that
compatibility floor stable across Zig upgrades. All archives include
`include/antfly.h` and the platform library under `lib/`;
`scripts/packaging/build_zig_release_archive.sh` builds the runtime and then the
`capi` target into the same archive prefix. GNU Linux archives compile in the
CUDA and PJRT/XLA backends, which discover their driver or plugin at runtime and
remain usable on CPU-only glibc hosts. PJRT/XLA use still requires a compatible
plugin supplied by the runtime environment.

The shell installer reads the glibc version with `getconf` and selects the GNU
archive only when the host meets its 2.28 compatibility floor. Older glibc,
musl, and unknown Linux libc environments use the portable musl archive. Set
`ANTFLY_LIBC=gnu` or `ANTFLY_LIBC=musl` to override selection, for example when
preparing an installation for a different host. In automatic mode it also
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
@antfly/cli-linux-x64
```

The existing unsuffixed Linux package names now contain the GNU builds and
declare `libc: glibc`; no additional npm packages need to be bootstrapped. This
is an explicit compatibility change from versions that placed musl builds in
those packages. Alpine and other musl users should install via `install.sh`.
The top-level `@antfly/cli` package exposes the `antfly` bin and selects the
package matching the host OS and CPU, while rejecting musl with that migration
guidance.

The main release workflow is the sole npm trusted publisher. Platform packages
publish first and the top-level selector publishes last. Each publish skips an
exact version only when the registry integrity matches the local tarball,
allowing a partially completed release to be retried safely without hiding
content drift or creating a selector whose platform package is absent.
