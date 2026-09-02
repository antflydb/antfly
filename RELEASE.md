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

Release is split into an untrusted request plane, a default-branch artifact
plane, and a default-branch promotion plane. Code loaded from a tag or manually
selected workflow ref never controls a builder or receives publication
credentials.

1. `.github/workflows/antfly-release-build.yml` and
   `.github/workflows/antfly-nightly.yml` emit only an untrusted JSON request.
   They never call a builder. `.github/workflows/antfly-release-build-controller.yml`
   is loaded through `workflow_run` from the default branch. It requires tag
   requests to match the pushed tag and commit, and nightly requests to have
   been dispatched on the default branch.
2. The build controller pins its exact commit and calls
   `.github/workflows/antfly-artifact-build.yml`, the sole reusable artifact
   builder, from that same commit. The requested source commit supplies only
   the versioned builder inputs.
   The controller first requires that source to declare the supported
   `scripts/release/build-contract.json` schema and all required build paths.
   With only read permission, it builds the canonical Zig archives,
   calls `.github/workflows/cli-package.yml`, extracts `install.sh` and
   `openapi.yaml` directly from the selected Git commit, and uploads the native
   archives, CLI snapshot, commit-bound source snapshot, and
   canonical `ReleaseSpec` (source commit, build-controller commit, channel,
   build contract, and all registry versions) as an Actions artifact. The
   workflow graph, nested reusable workflows, and controller-owned scripts all
   share that recorded build-controller identity.
3. Completion of the build controller triggers
   `.github/workflows/antfly-release.yml` through `workflow_run`. GitHub loads
   this privileged promotion workflow from the default branch. It checks the
   channel identity and source commit, combines the build outputs into one
   schema-versioned artifact ledger, verifies it, attests
   every payload file, and stores the exact bytes under immutable,
   content-addressed object-storage keys. Stable and next additionally stage
   those bytes on a draft GitHub Release; nightly deliberately does not create
   a GitHub Release.
4. The promotion controller separates the complete payload into
   ledger, runtime, and CLI scopes, and verifies source ancestry, attestations,
   exact scope membership, and every digest.
5. Policy-selected package registries, the Homebrew formula, and the single
   multi-architecture container image consume those verified scopes. Stable
   and RC releases publish npm and PyPI packages; nightly builds verify the same
   package snapshot but publish it only to R2 and the container registries. The
   container publisher is callable
   only by this controller, checks out the recorded controller commit, and
   consumes the GNU runtime archives built from the source commit in step 2.
   Its Dockerfile, Cloud Build configuration, platform policy, and registry
   code are therefore controller-owned rather than source-provided. The
   controller read-only preflights channel precedence before building, resolves
   the multi-platform OCI digest, and atomically creates a permanent
   `antfly/container-identities/<ledger>.json` record before copying
   ledger-addressed retention aliases to GAR and GHCR. Recovery resolves that
   per-release record independently of the channel's current version, restores
   its digest from either registry, and fails if both copies are gone; it never
   rebuilds a recorded OCI identity. Semantic version tags are create-or-verify
   identities, while only channel tags are mutable aliases.
6. Mutable install channels are transactions. After the build, a single
   read-only gate verifies the configured PyPI file set, npm package versions
   and dist-tag, and every OCI version and architecture tag; it also requires
   the Homebrew input to be prepared successfully. The compare-and-swap
   transaction then reserves the complete ledger-and-container identity before
   any PyPI, npm, Homebrew, or public container-tag write, so a known precedence or
   immutable-identity collision cannot be discovered after publishing a
   registry version. `scripts/release/channels.json`
   is the canonical policy for `stable`, `next`, and `nightly`: tag class,
   ordering rule, journal, package-registry eligibility, mutable aliases,
   GitHub visibility, and recovery source. Compare-and-swap journals in R2
   record each channel's `pending` and `current` release identity (tag, source
   commit, release-ledger digest, and OCI image digest). Build and preflight
   failures never create a pending transaction. After the commit boundary, a
   failed run must resume the same complete pending identity, and recovery
   cannot move a channel backward. Immediately before journal commit, the
   controller reads every policy-selected channel projection again and requires
   the enabled npm, PyPI, Homebrew, R2, GitHub release, GAR, and GHCR state to agree with the
   reserved identity. The journal is therefore a record of observed convergence,
   not merely an assertion that the publication steps ran.

The authenticated channel resolver is the only component that interprets
release-version syntax. It projects the accepted release identity into exact
npm, Python, and container registry versions. The controller requires those
projections to match the digest-verified release ledger, then exports the sealed
values. Preflight, publication, and final verification consume them verbatim;
they do not independently reinterpret canonical or legacy version spellings.

Channel bootstrap is fail-closed and happens exactly once. Before creating a
journal, the discovery controller reconciles every policy-selected,
version-bearing projection: all four npm packages when enabled, the R2 object alias, stable
GitHub `latest`, and the stable Homebrew formula. Missing projections may be
initialized, but present projections must name one identical release. After a
journal exists it is the channel authority and every present completed
projection must equal its `current` identity; a missing projection remains
repairable. While a transaction is pending, each projection may be missing,
`current`, or `pending`, which permits exact resumption without
accepting an unrelated or newer alias. Mutable mirrors are never used to
reconstruct history or silently override the journal. Only an explicit missing
release, package, dist-tag, object, or formula is empty state. Authentication,
rate-limit, malformed-response, and network failures stop promotion.
Container and npm operations use the typed adapters under
`scripts/release/registry/`. Registry lookups return only present or explicitly
missing; failures are a separate error path and can never authorize creation.
Container tags are copied only from digest-pinned sources and then
digest-verified through the same interface. Version tags reject an existing
different digest; channel tags may move only inside the channel transaction.
The permanent ledger-addressed record and OCI digest form the immutable
container identity. Final convergence is stricter than bootstrap: a missing
configured projection is a failed promotion, not empty state.

When enabled, npm platform packages publish before the top-level selector. Existing npm or
PyPI files are skipped only when their registry digest matches, and PyPI must
contain exactly the ledger-defined filename-to-digest set before channel
promotion can continue, so a partial publication is safe to retry without
hiding content drift or accepting untracked distributions. An existing npm
version is resumable only if its requested dist-tag also points to that exact
version; trusted-publishing recovery otherwise stops with an explicit dist-tag
repair requirement. Existing GitHub release assets are likewise accepted only
when byte-identical on the normal path. Recovery may replace a corrupt asset
only after the operator-supplied ledger digest, complete payload, and
provenance attestations all verify against the immutable R2 copy. GitHub
release visibility is also one-way: a retry may repair assets on a published
release but never returns it to draft state.

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
registry job access. Stable and next recovery try the GitHub Release and then
the immutable R2 version prefix. Each source must independently contain exactly
the supplied ledger and every byte it names before it is accepted. If R2
repairs a missing or corrupt GitHub payload, the controller restores the
verified bytes and removes unledgered assets before publishing the release.
Nightly uses R2 as its only policy-selected source.

`.github/workflows/antfly-container.yml` has only a `workflow_call` entry point.
It accepts only the default-branch promotion controller's normal `workflow_run`
or recovery dispatch events, rechecks source ancestry and the release-ledger
digest, and consumes the verified GNU archives. Cloud Build uses an immutable
source-commit-and-ledger-addressed artifact URI and has no mutable or
ABI-ambiguous default input.

Temporary run-scoped container tags are build staging only. A
`release-ledger-<ledger digest>` tag in both GAR and GHCR retains the digest
bound by the permanent per-release record; either copy can repair the other by
digest. If both are lost, recovery fails explicitly instead of trying to
reproduce bytes from mutable package repositories or build tools. Version tags
are immutable after their first publication, while channel tags remain
transaction-controlled aliases. Registry retention may remove
run-scoped staging tags after the release. There is deliberately no automatic
rollback entry point for mutable channels. A
rollback requires a separately reviewed administrative change to the channel
journal and aliases, rather than disguising an old-release recovery as a new
promotion.

The canonical ABI, archive, package, wheel, backend, and consumer matrix lives
in `scripts/release/platforms.json`; channel behavior lives in
`scripts/release/channels.json`. Release jobs, CLI packaging, Homebrew, and
promotion and retention read those policies instead of maintaining independent
conditionals or platform tables. Python
dependencies used by the release control plane are exact and hash-locked in
`scripts/release/requirements.lock`; Node and npm versions are exact as well.
Every workflow declares an explicit `GITHUB_TOKEN` permission baseline, and all
external GitHub Actions are pinned to full commit SHAs. CI enforces both rules
repository-wide, and reviewed Dependabot pull requests advance those pins. When
this policy lands, the repository-level default workflow permission must be set
to `read` and kept there; explicit job-level grants are the only supported way
to obtain write access.

Release metadata and object-storage publishing are implemented as explicit
scripts under `scripts/release/`. `scripts/release/test.sh` is the portable,
single CI entry point for release packaging, installer, recovery, registry, and
workflow contract tests; its test discovery intentionally picks up new
`test_*.py` files. `make release-scripting-test` delegates to the same script as
a local convenience.

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
  backward promotion, provides a read-only preflight, and makes an interrupted
  commit resumable only by the same complete release identity. A completed
  transaction records its timestamp in the journal and writes an immutable,
  ledger-addressed receipt under `antfly/release-history/`; retries repair a
  missing receipt without inventing a new completion time.
- `release_container_state.py` create-once binds each release ledger to its OCI
  digest independently of mutable channel history, so later recovery can find
  the original container without rebuilding it.
- `registryctl.py` is the workflow-facing command for the typed npm and
  container adapters. Provider behavior is tested with injected responses for
  missing objects, content drift, authentication and network failures, and
  post-copy verification.
- `prepare_npm_promotion.py` read-only verifies every ledger-defined npm
  package and required channel tag against the authenticated npm registry
  version as part of the global publication gate.
- `recover_release_payload.py` restores an exact ledger from the channel's
  ordered immutable mirrors and accepts a mirror only after the ledger digest,
  tag, commit, exact member set, sizes, and hashes all verify. GitHub release and
  asset listings are fully paginated, including draft releases.
- `download_objectstorage.py` provides the authenticated R2 reader and exact,
  hash-verified ledger-member restoration used by bootstrap and recovery. Its
  standalone mode requires the expected tag, commit, and ledger digest.
- `publish_objectstorage.py` first writes content-addressed and versioned keys
  with compare-or-fail semantics and seals the version prefix only when it
  contains exactly the ledger-defined files. Channel namespaces are exact,
  policy-defined pointer sets: `next` and `nightly` contain only
  `metadata.json`, while `latest` also contains the controller-owned,
  release-independent installer bootstrap. Promotion prunes legacy files before
  atomically replacing metadata, so interrupted releases cannot expose a mixed
  channel payload. The release workflow
  currently uses the S3-compatible path for Cloudflare R2, but the script also
  has GCS and local modes for future storage backends and dry-run smoke tests.
- `release_gc.py` plans retention from immutable ledgers plus channel journals.
  It fails closed on malformed state, permanently marks stable and channel
  `current`/`pending` releases, retains nightlies for 30 days or the newest 10,
  and retains prereleases until 90 days after their matching stable release's
  immutable completion receipt. Unschematized ledgers from the previous release
  pipeline have an explicit legacy decoder. Its sweep removes only expired
  version objects and content-addressed objects not shared by a retained ledger.
- `release_registry_gc.py` consumes that exact plan, verifies that no mutable
  container channel or unplanned registry tag still reaches a selected digest,
  and removes the expired manifest list and architecture images from GAR and
  GHCR.
  Only after registry cleanup succeeds does the workflow remove the R2
  container-identity record and release objects.

## Version Behavior

Stable tags use `vX.Y.Z`; prerelease tags use the canonical
`vX.Y.Z-alpha.N`, `vX.Y.Z-beta.N`, or `vX.Y.Z-rc.N` spellings. Nightly snapshots use
`v0.0.0-dev.<GitHub run ID>`. A nightly is a channel, not a cadence: the
workflow is manual today, and a schedule or default-branch trigger can be added
later without changing the artifact or promotion model.

Release identity is deliberately narrower than generic SemVer: the core always
contains three components and build metadata is rejected because it cannot be
represented by every publication target (notably container tags). Historical
spellings such as `rc2`, `pre.2`, and `preview2` can be read from existing
journals during recovery, but cannot create a new release candidate.

Run a snapshot for the current default-branch head with
`gh workflow run antfly-nightly.yml`. To reproduce a snapshot from a specific
default-branch commit, add `-f source_commit=<40-character-commit>`.

Stable releases publish:

- GitHub Release artifacts
- R2 release artifacts
- the `latest` R2 metadata pointer and stable installer bootstrap
- Zig Homebrew formula `antfly`
- npm CLI packages with dist-tag `latest`
- PyPI CLI wheels
- container tags `<version>` and `latest`

RC releases publish:

- GitHub Release artifacts marked prerelease
- R2 release artifacts
- the `next` R2 metadata pointer
- npm CLI packages with dist-tag `next`
- PyPI CLI wheels using PEP 440 prerelease versions, for example
  `0.2.0-rc.1` becomes `0.2.0rc1`
- container tags `<version>` and `next`

RC releases do not update the `latest` R2 channel, Homebrew stable formula, or
container `latest` tag.

Nightly snapshots publish:

- immutable R2 release artifacts and the `nightly` R2 metadata pointer
- container tags `<version>` and `nightly`

Nightlies do not publish to npm, PyPI, or Homebrew and do not create GitHub
Releases. Their immutable recovery source is R2. npm tarballs and Python wheels
are still produced and ledger-verified by the common build, but are retained as
release artifacts rather than published package-registry versions.

## Object Retention

Stable R2 releases are retained forever. A nightly remains recoverable for at
least 30 days and the newest 10 nightlies are retained regardless of age.
Prereleases remain until a matching stable completion receipt exists and has
aged 90 days. An uploaded or draft stable payload does not start this clock.
The current and pending identities in every channel journal, plus each mutable
R2 metadata alias, are always protected.

`.github/workflows/antfly-release-gc.yml` computes and archives a plan weekly
without deleting anything. To inspect a plan immediately, dispatch `Release
object retention` with its default `apply=false`. To apply retention, dispatch
it with `apply=true`. The protected `container-publish` approval job finishes
before the apply job enters the short release-promotion concurrency section.
Inside that lock, application recomputes the plan, removes only container
digests with no retained or channel references, checks the journal and alias
ETags, and then removes R2 objects. The GC never deletes stable releases,
shared content-addressed objects, or npm/PyPI versions. Immutable completion
receipts remain as compact audit history after payload deletion. Existing
`termite/` objects are deliberately outside this Antfly release policy and are
retained until that retired namespace receives an explicit migration or bucket
lifecycle decision.

Package registries are immutable. If an RC publish reaches npm or PyPI, the same
version cannot be republished after recreating the tag; cut the next RC instead.
