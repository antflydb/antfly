# Docs and design review exception

AJ (CTO) requested this exception on 2026-10-30 for all human contributors with
current write access to `antflydb/antfly` (including maintainers/admins). No
repository roles or branch-protection bypass allowances change.

The metadata-only `Docs and design approval` workflow approves same-repository,
non-draft PRs into main only if every changed file is within:

- Root `README.md` or `CHANGELOG.md`.
- Markdown/MDX under `docs/`, including `docs/design/`, excluding `docs/security/`.
- `ts/packages/design-system/src/`: design components, styles, tokens and fonts.

Agent instruction files (`AGENTS.md`, `CLAUDE.md`, `GEMINI.md`), Cloudflare control
files (`_headers`, `_redirects`, `.assetsignore`), symlinks, executable blobs and
submodules are excluded. Package metadata, dependencies/lockfiles, build/release
configuration, CI, SDKs, dashboard apps, database/runtime code, license files and
this policy require normal review. Renames must stay inside the allowed scope;
mixed PRs never receive this automatic approval.

This delegates design-system source maintenance, including executable UI code;
it is not a claim that every eligible file is inert. The approval job itself
executes trusted base policy only and reads GitHub metadata, never PR code. It
checks the PR author's current write access, complete changed-file enumeration,
regular file modes and the exact head/base. API errors and concurrent changes
fail closed. Reviews explicitly record the authorized policy exception rather
than claim human review.

Contributors post `/ci run <full-head-sha>` and merge after required CI succeeds.
The existing `PR CI` and `Apache license boundary` checks, latest-push approval,
stale-review dismissal, code-owner review outside the exception, up-to-date
branches and conversation resolution remain required. There is no automatic
merge or automatic database deployment.

## Approval credential and activation

Keep Antfly's **Allow GitHub Actions to create and approve pull requests** setting
**disabled**. The default `GITHUB_TOKEN` has read-only permissions in this job.
A separate GitHub App submits approvals using a short-lived installation token,
scoped to Antfly and revoked by the token action when the job finishes.

Create the organization-owned [`antfly-docs-approver` App](https://github.com/organizations/antflydb/settings/apps/new?name=antfly-docs-approver&description=Scoped+docs+and+design+review+exceptions+for+Antfly+repositories&url=https%3A%2F%2Fgithub.com%2Fantflydb&public=false&webhook_active=false&contents=read&pull_requests=write) with only Contents: read and Pull requests: write
(repository Metadata: read is implicit). Disable webhooks; no event subscriptions
or callback URL are needed. Install the shared App on **only `antflydb/colony` and `antflydb/antfly`**.
Each workflow must request a token for its own repository only; the Antfly
workflow enforces its separate docs/design policy. The registration link prefills
the name and permissions; verify those values before creation. Give it no
administration, workflow, content-write or branch-bypass permissions. Generate a
private key and note the App ID from its settings page.

Before storing credentials, configure the `docs-design-approval` GitHub
**environment** with selected deployment branches/tags: exactly the **branch**
`main`, and no tags, wildcard branches or PR refs. Use custom branch policies,
not the setting allowing all protected branches. No required reviewer is needed
for this environment; the main-only restriction is the credential boundary.
`pull_request_target` runs use the trusted base branch ref, so eligible PR events
can use the environment while arbitrary branch workflows cannot.

Store these values in that environment only:

- Variable `DOCS_APPROVAL_APP_ID`: the dedicated App ID.
- Secret `DOCS_APPROVAL_APP_PRIVATE_KEY`: the generated PEM private key.

Do not create repository- or organization-level copies of that secret: those
would be accessible from untrusted branch workflows. With a locally downloaded
key, use the authenticated GitHub CLI (replace the placeholders):

```sh
gh variable set DOCS_APPROVAL_APP_ID --repo antflydb/antfly \
  --env docs-design-approval --body APP_ID
gh secret set DOCS_APPROVAL_APP_PRIVATE_KEY --repo antflydb/antfly \
  --env docs-design-approval < /path/to/app-private-key.pem
```

Keep `.github/` workflow and policy changes founder-owned through `CODEOWNERS`. After
founder review and merge, exercise an eligible PR and a mixed-change PR. Confirm
that the dedicated App approves only the eligible head and CI remains required.
A branch workflow requesting the same environment must be blocked by GitHub's
branch restriction. Leave existing branch protection and Actions defaults intact.
Missing credentials or environment rejection fail the job without an approval.

To revoke, disable the workflow, revoke/rotate the App key or uninstall the App,
and dismiss outstanding exception approvals. Include this exception, App
installation permissions and environment branch restrictions in access reviews.
