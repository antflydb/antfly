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

1. Push a stable or RC Antfly tag, or manually dispatch the `Nightly request`
   workflow on the default branch. Both entry points emit only an untrusted
   request. A `workflow_run` controller loaded from the default branch validates
   that request and calls the same read-only reusable artifact build at its
   exact controller commit.
2. The reusable CLI packaging workflow consumes the native GitHub Actions
   artifacts from that same build and builds all npm tarballs and Python wheels
   once.
3. The workflow records their hashes, source commit, npm version, and PEP 440
   Python version in `cli-snapshot.json`. The tag and manual-request workflows
   have read-only permissions and stop after uploading their request; the
   default-branch build controller uploads the runtime archives, CLI snapshot,
   and release identity as Actions artifacts.
4. GitHub Release assets and versioned object-storage objects are immutable:
   the default-branch promotion workflow combines the build outputs into a
   schema-versioned `artifacts.json` ledger whose entries belong to explicit
   `runtime`, `cli`, or `support` scopes, verifies it, and attests every exact
   payload file. Retries accept an existing object only when its digest matches.
   Object storage also retains content-addressed objects under
   `sha256/<digest>/`.
5. GitHub starts that promotion through `workflow_run`, so the privileged code
   is always loaded from the default branch. The controller verifies the
   source commit's declared build-contract schema and consumes its immutable
   `ReleaseSpec`, which records the source commit and the default-branch
   controller that built the artifacts. Promotion records a second, distinct
   `promotion_controller_commit`; every privileged job checks out that one
   workflow revision. The controller then verifies the
   complete release bundle before promoting its CLI scope to policy-selected
   package registries, its
   GNU runtime scope to the single container image, and stable archives to
   Homebrew. The controller first performs a read-only channel precedence
   preflight. Container images are built under run-scoped staging tags,
   resolved to OCI digests, bound once to permanent ledger-addressed records,
   and retained by ledger aliases in GAR and GHCR. A single read-only gate then
   checks all immutable PyPI, npm, and OCI destinations and requires a
   successfully prepared Homebrew input before the complete digest-bound
   identity is reserved in the channel journal. Semantic
   version tags are create-or-verify and cannot move to another digest; only
   channel tags are mutable. Every copy reads from a digest-pinned source and
   is verified afterward.
   The channel resolver alone interprets release syntax and emits exact npm,
   Python, and container registry versions. The promotion controller binds them
   to the digest-verified ledger, and every downstream check and publisher
   consumes those sealed projections, including legacy recovery, rather than
   parsing the release tag again.
   npm `latest` or `next` when enabled, plus the channel's container and R2
   aliases and policy-selected GitHub visibility, are committed through
   compare-and-swap channel transactions. npm, PyPI, and Homebrew are omitted for
   nightlies. Immediately before committing the journal, the controller reads
   back every configured projection: all npm dist-tags, the exact PyPI file set
   when enabled, Homebrew when enabled, the R2 alias, GitHub publication mode,
   and all GAR/GHCR version and channel tags. Any missing or divergent
   projection leaves the transaction pending for exact retry.
   R2 channels do not mirror a mutable copy of the release payload. They own an
   exact metadata pointer; stable additionally owns a small controller-defined
   bootstrap at `latest/install.sh` that resolves the pointer and delegates to
   the immutable, versioned installer. Legacy alias objects are removed before
   the metadata pointer moves.
   When enabled, npm additionally verifies the requested dist-tag, so retries cannot conceal
   content or channel drift. Recovery restores the permanently recorded digest
   from either retention registry and fails if both copies are gone. It never
   rebuilds a permanently recorded container identity.
   Container assembly uses promotion-controller-owned Docker and
   Cloud Build inputs plus the verified GNU archives; it never executes release
   tooling from `source_commit`.
   Native archives remain available under
   `https://releases.antfly.io/antfly/v0.2.0/` for direct installation.

Stable and RC releases publish the verified CLI packages to npm and PyPI.
Nightlies build and ledger-verify those same packages, but publish only the R2
release payload and container image; package registries are not used as
snapshot storage.

Release-object retention is journal-aware. Stable releases are permanent;
nightlies are retained for 30 days or for the newest 10 snapshots, whichever
keeps more; and RC/alpha/beta artifacts remain until 90 days after the matching
stable release's immutable completion receipt. Merely uploading stable bytes or
creating a draft does not start that clock. Channel `current` and `pending`
identities always override those windows. `Release object retention` emits a
read-only plan every Monday. A manual dispatch with `apply=true`, protected by the
`container-publish` environment, completes before the short release/GC lock is
acquired. The apply phase recomputes the plan, verifies that no channel still
reaches an expired container digest, removes its GAR and GHCR images, checks
that channel journals and aliases did not change, and only then removes version
objects, unshared content-addressed objects, and their R2 container-identity
record. Compact completion receipts remain as permanent audit history.

For recovery, send the same repository dispatch with an existing release tag
and the SHA-256 of its `artifacts.json` asset:

```sh
gh api --method POST repos/antflydb/antfly/dispatches \
  -f event_type=promote-cli-release \
  -f 'client_payload[version]=v0.2.0' \
  -f 'client_payload[ledger_sha256]=<64-character-sha256>'
```

Recovery tries the channel's policy-ordered immutable mirrors (GitHub Release,
then R2 for stable and next; R2 for nightly) and accepts one only after the
supplied ledger digest and exact ledger-member set verify. It then verifies every
GitHub attestation against the expected release-workflow signer, tag ref,
repository, and source-commit digest and promotes those exact bytes. It never
checks out operator-selected source to rebuild or publish registry artifacts.
When R2 supplies a stable or next payload, recovery restores missing or corrupt
GitHub assets and removes unledgered assets only after the supplied ledger,
complete payload, and provenance attestations verify. Recovery may also repair
object-storage, PyPI, and container version aliases for a saved release, but npm `latest`/`next` and
stable Homebrew, container, object-storage,
and GitHub `latest` channels move only forward. An interrupted promotion leaves
a journaled pending identity and only that exact tag, source commit,
release-ledger digest, and container digest may resume it. A published GitHub
release is never changed back to draft.

Nightly recovery uses the general channel event because nightlies deliberately
have no GitHub Release. Supply its exact source commit and ledger digest; the
controller restores the immutable versioned payload from R2 before running the
same attestation and ledger checks:

```sh
gh api --method POST repos/antflydb/antfly/dispatches \
  -f event_type=promote-release-channel \
  -f 'client_payload[channel]=nightly' \
  -f 'client_payload[version]=v0.0.0-dev.<run-id>' \
  -f 'client_payload[commit]=<40-character-source-commit>' \
  -f 'client_payload[ledger_sha256]=<64-character-sha256>'
```

The channel contract is centralized in `scripts/release/channels.json` rather
than encoded as version-string tests throughout the workflows. The same policy
owns each channel's publication destinations and object-retention rule.
`scripts/release/test.sh` is the canonical local and CI suite for packaging,
installer, registry, recovery, and release-workflow contracts; it discovers all
`test_*.py` modules under both `scripts/packaging` and `scripts/release`.
`make release-scripting-test` is a convenience wrapper for the same script.
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

Promotion uses no workflow-wide concurrency lock. Immutable R2 sealing and the
journal reservation share a short job-level lock with retention; protected
environment waits do not hold it. The R2 journal is the durable per-channel
transaction boundary after reservation, so an interrupted npm or PyPI approval
wait preserves one exact resumable identity while unrelated channels and GC
continue. The container approval is completed during preflight, and container,
object-storage, GitHub, and completion jobs do not wait on another environment
after the journal is reserved.

Container staging writes only digest-addressed candidates and therefore runs
before approval without selecting a public alias. `container-publish` is used
exactly once, by the preflight job, as the human gate before the transaction is
reserved. Its required reviewers and allowed `main`/legacy tag refs are
declared in `scripts/release/github-environments.json`. Both release preflight
and GC planning compare the live environment with that contract. Apply an
intentional configuration change with `python3
scripts/release/github_environment.py apply --repository antflydb/antfly` using
a repository-administrator GitHub token.

Release retention plans are approval artifacts, not advisory previews. Their
canonical SHA-256 covers policy, retained and expired identities, R2 keys,
container digests, and per-ledger container records. Apply recomputes under the
release-storage lock and aborts unless that contract is unchanged. Container
records are collected independently from their shared OCI digest, while a new
release missing its required record is retained for repair.

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

For published prerelease tags, npm uses the release version directly. Python
wheels use PEP 440 equivalents, for example `v0.2.0-rc.1` becomes `0.2.0rc1`.
Stable npm releases publish with the `latest` dist-tag. Prerelease npm versions
publish with the `next` dist-tag, so `npm install -g @antfly/cli` stays on the
latest stable release and `npm install -g @antfly/cli@next` can install RC
builds. Nightly package artifacts remain in R2 and have no npm dist-tag.

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
