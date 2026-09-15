# CI approval and test selection

PR CI requires a human approval for each commit and each new run. Labels select
additional suites; they never authorize compute on their own. Antfly and Colony
use the same controller implementation and repository-specific suite maps.

## Run CI on a PR

1. Mark the PR ready for review. Draft PRs do not start test runners.
2. Add any optional suite labels from the table below.
3. A human with repository write, maintain, or admin access posts a **new** comment:

   ```text
   /ci run <full 40-character PR head SHA>
   ```

   Obtain the SHA with `gh pr view NUMBER --json headRefOid --jq .headRefOid`.
   The command must be the entire comment. Short SHAs and edited comments are
   rejected. Any human with repository write, maintain, or admin access can
   approve, including outside collaborators. Org membership alone does not grant
   approval rights. No per-user allowlist, GitHub App, or additional credential is
   required; permission checks use the automatically provided `GITHUB_TOKEN`.
4. Follow **Approved PR CI** in Actions and the **PR CI** check on the PR head.

Posting a new approval cancels the previous PR run and starts another attempt.
Use a fresh comment to retry failed tests; Actions' **Re-run jobs** is not an
approval and PR worker jobs reject subsequent run attempts.

| Repository | Label | Additional suite |
| --- | --- | --- |
| Antfly | `ci:scale-tests` | Corpus-scale full-text correctness, over 1M chunks |
| Antfly | `ci:gpu` | L4 Spot CUDA build and smoke canary |
| Antfly | `ci:darwin-release` | Darwin ARM64 ReleaseFast archive diagnostic |
| Colony | `benchmark-preview` | Website benchmark preview build and publication |

For example, add `ci:gpu` and `ci:scale-tests`, then post one approval comment to
run normal CI plus both optional suites. GPU `inference-e2e` remains an explicit
manual default-branch option in **Zig Inference L4 Spot Canary**. The PR label
selects the bounded `smoke` scope.

Normal Antfly CI includes policy tests, SDK checks, and the existing Zig PR/base
validation. SDK and Zig workflows retain their change classification. Proxy and
operator suites retain their path selection. Colony selects Go, Vitest,
Playwright, website checks, and infrastructure preview by their existing paths;
policy tests always run. Renamed files consider both paths. Diffs at GitHub's
3,000-file API limit select all normal suites rather than silently losing coverage.

## When approval expires

| Event | Result |
| --- | --- |
| Open or update a draft | No test runners; `PR CI` reports draft/approval required |
| Mark ready or reopen | Await a fresh approval |
| Push a new head commit | Cancel previous PR compute; require approval for the new SHA |
| Add or remove a suite label | Invalidate approval and cancel the run; do not start tests |
| Add an unrelated label | No change to CI |
| Convert to draft or close | Invalidate approval and cancel PR compute |
| Edit or delete the approval comment | Invalidate that approval |
| Base SHA/ref changes | Admission and completion reject the old snapshot |
| Suite fails, is cancelled, or does not report success | `PR CI` cannot pass |

The controller reads live PR state. It checks that suite labels still match the
approval event, and records the head SHA, base SHA/ref, labels, selected suites,
and approver. Admission revalidates this record before suites start. Completion
revalidates it again before publishing success. A result from an obsolete run
cannot overwrite the current approval. Cancellation is asynchronous: an already
running job may take time to stop. Base advancement alone does not emit a PR
webhook; require branches to be up to date when enforcing merge protection.

The test checkout is the approved **head SHA**, not a moving branch or synthetic
merge commit. Existing merge-group tests cover integration when using a merge
queue; they have a separate execution policy below.

## Implementation and trust

- `.github/workflows/pr-ci-controller.yml` handles PR lifecycle events, approval
  comments, and completion of the orchestrator. It loads only default-branch
  controller code, never PR code. `pull_request_target` is metadata-only.
- `.github/workflows/pr-ci.yml` consumes one recorded approval, calls selected
  reusable workflows, and requires every selected suite to succeed.
- `.github/workflows/pr-ci-admission.yml` verifies the approval and checkout for
  each suite on a small GitHub-hosted runner before its test jobs are eligible.
- `.github/scripts/pr-ci.cjs` implements the controller; `pr-ci-config.json`
  contains the repository's suite labels and path patterns. Keep controller and
  test code identical in both repositories when changing policy.
- `.github/workflows/pr-ci-policy.yml` exercises the policy tests using the
  approved checkout. Test jobs have no Actions/check-write permission. Only the
  trusted control plane writes `PR CI` on the actual PR commit, because a normal
  dispatched workflow's checks attach to its default-branch dispatch SHA.

The small controller/admission jobs consume some GitHub-hosted minutes, including
on draft PR events. No ARC, GPU, or test job starts from those events without a
valid approval. The controller does not keep a runner waiting for a person.

PR execution is restricted to same-repository PRs because these workflows use
self-hosted runners. Ordinary test suites do not inherit repository secrets.
Colony's approved infrastructure and benchmark previews retain the credentials
they need; approving these suites also approves running that PR code with those
credentials. Review those changes before authorizing them.

GitHub identifies the account behind a comment, not the person or software using
its credentials. Bot accounts are rejected, but an agent using a human's token is
indistinguishable from that human. Use separate bot credentials for automation and
keep human approval credentials out of agents. Repository administrators and
writers who can change workflow definitions remain trusted; this is a cost gate,
not a sandbox against malicious repository maintainers.

## Main, schedules, manual runs, and merge queues

Existing main/master push, schedule, release, and merge-group triggers retain
their execution policies. They are separate sources of cost. In particular,
Antfly's scheduled scale tests and nightly/full Zig runs still execute without a
PR comment. Manual test dispatches use the default branch; PR branch testing goes
through the approval controller.

The controller serializes PR state changes with admission using
`concurrency.queue: max`, so a burst of events does not replace pending approval
or invalidation jobs. GitHub caps that queue at 100. If GitHub cancels a controller
or admission job, no success is inferred; inspect the run and submit a fresh
approval. The narrow actionlint exceptions cover this documented syntax, which
actionlint 1.7.12 does not yet recognize.

## Rollout and merge enforcement

1. Merge the controller, reusable workflows, configuration, tests, and docs
   together into each repository's default branch. Default-branch event handlers
   cannot be exercised by merely pushing this implementation branch. Existing
   queued/running workflows from before rollout are not retroactively gated.
2. Create the suite labels above. Approvers use their existing repository write,
   maintain, or admin access. No extra secrets, GitHub App, personal access token,
   per-user allowlist, or approval environment is required: `GITHUB_TOKEN` handles
   permission checks, dispatch, and check updates.
3. Verify with a small non-draft PR: no tests before approval, a new full-SHA
   comment starts selected tests, and a subsequent push requires fresh approval.
   Verify draft conversion and adding/removing suite labels cancel an active run.
4. Configure branch protection/rulesets to require **PR CI**, from the GitHub
   Actions app, and require the branch to be up to date. Replace old PR-specific
   required check names after verifying the new check. Do not require optional
   GPU/scale job names globally; the aggregate check requires them when selected.
5. If enabling a merge queue, configure its required checks separately with the
   existing `merge_group` workflows. `PR CI` is a PR-head approval check; this
   controller does not publish it on merge-group SHAs. Do not enable a ruleset
   that requires a missing check on the merge queue.

At inspection on 2026-09-15, Antfly was public with no main-branch protection and
no repository rulesets. Colony was private and the organization reported GitHub
Free. The workflow cost gate works without a paid approval environment, but
GitHub's private-repository protection features require an eligible paid plan
before Colony can enforce the check at merge time. Native environment required
reviewers are also unavailable for private repositories on Free, Pro, and Team.
Repository settings and plan changes are separate from these source changes.

## Local verification

```sh
node --test .github/scripts/pr-ci.test.cjs
actionlint -shellcheck=''
python3 -m unittest discover -s scripts/ci -p 'test_*.py'
```

The controller tests mock GitHub responses and cover authorization, draft/fork
rejection, immutable checkouts, suite selection, replay rejection, cancellation,
and late/failed completion. They do not provision runners or run GPU/scale tests.

## GitHub references

- [Workflow events and privileged triggers](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows)
- [GITHUB_TOKEN dispatch behavior](https://docs.github.com/en/actions/concepts/security/github_token)
- [Reusable workflow permissions and concurrency](https://docs.github.com/en/actions/reference/workflows-and-actions/reusing-workflow-configurations)
- [Concurrency queues](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency)
- [Environment feature availability](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments)
- [Protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches)
