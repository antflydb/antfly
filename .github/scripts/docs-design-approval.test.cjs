'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {allowedPath, eligible, validateFiles, main} = require('./docs-design-approval.cjs');

function fixture() {
  const repo = {full_name: 'antflydb/antfly'};
  const pr = {state: 'open', draft: false, user: {login: 'drewlanenga', type: 'User'},
    changed_files: 1, head: {sha: 'a'.repeat(40), repo}, base: {sha: 'b'.repeat(40), ref: 'main', repo}};
  const files = [{filename: 'docs/guide.md', status: 'modified'}];
  const blob = {path: 'docs/guide.md', mode: '100644', type: 'blob'};
  const trees = [{tree: [{...blob}]}, {tree: [{...blob}]}];
  const comparison = {base_commit: {sha: pr.base.sha}, merge_base_commit: {sha: pr.base.sha}, files};
  const comparisons = [], treeReads = [];
  const reviews = [], approvals = [], notices = [];
  let permission = 'write', reads = 0;
  const permissionLookups = [];
  const github = {rest: {
    pulls: {
      get: async () => {
        reads++;
        return {data: structuredClone(pr)};
      },
      listFiles: async () => {throw new Error('Mutable PR file enumeration is forbidden');},
      listReviews: async () => reviews,
      createReview: async args => approvals.push(args),
    },
    repos: {compareCommitsWithBasehead: async args => {
      comparisons.push(args); return {data: structuredClone(comparison)};
    }, getCollaboratorPermissionLevel: async args => {
      permissionLookups.push(args.username); return {data: {permission}};
    }},
    git: {getTree: async args => {
      treeReads.push(args.tree_sha);
      return {data: trees[args.tree_sha === comparison.merge_base_commit.sha ? 0 : 1]};
    }},
  }, paginate: async (method, args) => method(args)};
  const context = {repo: {owner: 'antflydb', repo: 'antfly'}, eventName: 'pull_request_target',
    payload: {pull_request: {number: 7}}};
  const core = {notice: message => notices.push(message)};
  const reviewerLogin = 'antfly-docs-approver[bot]';
  return {pr, files, trees, comparison, comparisons, treeReads, reviews, approvals, notices, permissionLookups, github, context,
    run: () => main({github, context, core, reviewerLogin}),
    setPermission: value => {permission = value;}, getReads: () => reads};
}

test('permits documentation and design-system source', () => {
  for (const path of ['README.md', 'CHANGELOG.md', 'docs/guides/start.mdx',
    'docs/design/architecture.md', 'docs/reference/query.mdx',
    'ts/packages/design-system/src/components/button.tsx',
    'ts/packages/design-system/src/tokens/colors.css',
    'ts/packages/design-system/src/fonts/aeonik/font.ttf']) {
    assert.equal(allowedPath(path), true, path);
  }
});

test('rejects privileged paths, configs, dependencies, agents and unrelated applications', () => {
  for (const path of ['docs/security/access-policy.md', 'docs/CLAUDE.md', 'docs/AGENTS.md',
    'docs/a/../security/x.md', 'docs//guide.md', '/docs/guide.md', 'docs/run.sh',
    '.github/workflows/docs-design-approval.yml', '.github/docs-design-approval.md',
    'zig/pkg/antfly/src/main.zig', 'zig/lib/raft/src/log.zig', 'go/pkg/sdk/client.go',
    'ts/packages/design-system/package.json', 'ts/packages/design-system/vite.config.ts',
    'ts/packages/design-system/tsconfig.json', 'ts/packages/design-system/src/AGENTS.md',
    'ts/packages/design-system/src/assets/_headers', 'ts/packages/design-system/src/assets/_redirects',
    'ts/packages/design-system/src/assets/.assetsignore',
    'ts/packages/sdk/src/client.ts', 'ts/pnpm-lock.yaml', 'scripts/release/tag.sh',
    'LICENSE', 'RELEASE.md', 'AGENTS.md', 'ts/apps/antfarm/src/main.tsx']) assert.equal(allowedPath(path), false, path);
});

test('rejects missing authors, bots, forks, drafts, closed PRs and other target branches', () => {
  for (const mutate of [f => {f.pr.user = null;}, f => {f.pr.user.type = 'Bot';},
    f => {f.pr.head.repo = {full_name: 'drewlanenga/antfly'};}, f => {f.pr.head.repo = null;},
    f => {f.pr.draft = true;}, f => {f.pr.state = 'closed';}, f => {f.pr.base.ref = 'release';}]) {
    const f = fixture(); mutate(f); assert.equal(eligible(f.pr), false);
  }
});

test('approves the inspected head and explicitly records the policy exception', async () => {
  const f = fixture(); await f.run();
  assert.equal(f.getReads(), 2);
  assert.equal(f.approvals.length, 1);
  assert.equal(f.approvals[0].commit_id, f.pr.head.sha);
  assert.equal(f.approvals[0].event, 'APPROVE');
  assert.match(f.approvals[0].body, /not a human code review/);
});

test('mixed paths, unsafe rename origins and incomplete pagination never approve', async () => {
  for (const mutate of [
    f => {f.files.push({filename: 'infra/main.go', status: 'added'}); f.pr.changed_files++;},
    f => {f.files[0].status = 'renamed'; f.files[0].previous_filename = '.github/workflows/ci.yml';},
    f => {f.files[0].status = 'renamed';},
    f => {f.files[0].status = 'copied';},
    f => {f.pr.changed_files = 2;}, f => {f.pr.changed_files = 3000;},
    f => {f.pr.changed_files = 0;}, f => {f.pr.changed_files = undefined;},
    f => {f.files.push({...f.files[0]}); f.pr.changed_files++;},
    f => {f.trees[1].truncated = true;}, f => {f.trees[0].truncated = true;},
  ]) {
    const f = fixture(); mutate(f); await f.run(); assert.equal(f.approvals.length, 0);
  }
});

test('symlinks, executable blobs and submodules cannot use the exception', () => {
  for (const index of [0, 1]) {
    for (const [type, mode] of [['blob', '120000'], ['blob', '100755'], ['commit', '160000']]) {
      const f = fixture(); Object.assign(f.trees[index].tree[0], {type, mode});
      assert.equal(validateFiles(f.pr, f.files, f.trees), false);
    }
  }
});

test('ordinary additions, deletions and allowed renames are supported', () => {
  for (const status of ['added', 'removed', 'renamed']) {
    const f = fixture(); f.files[0].status = status;
    if (status === 'added') f.trees[0].tree = [];
    if (status === 'removed') f.trees[1].tree = [];
    if (status === 'renamed') {
      f.files[0].previous_filename = 'docs/old.md'; f.trees[0].tree[0].path = 'docs/old.md';
    }
    assert.equal(validateFiles(f.pr, f.files, f.trees), true, status);
  }
});

test('any human writer, maintainer or administrator is eligible', async () => {
  for (const login of ['drewlanenga', 'ajroetker', 'bpopadiuk', 'timkaye11', 'another-writer']) {
    for (const permission of ['write', 'maintain', 'admin']) {
      const f = fixture(); f.pr.user.login = login; f.setPermission(permission);
      await f.run();
      assert.equal(f.approvals.length, 1, `${login}: ${permission}`);
      assert.deepEqual(f.permissionLookups, [login]);
      assert.ok(f.approvals[0].body.includes(`@${login}`));
    }
  }
});

test('readers, triagers, outsiders and revoked writers cannot receive approval', async () => {
  for (const permission of ['read', 'triage', 'none']) {
    const f = fixture(); f.setPermission(permission);
    await assert.rejects(f.run(), /does not have repository write access/);
    assert.equal(f.approvals.length, 0);
  }
});

test('API errors fail closed', async () => {
  const f = fixture(); f.github.rest.repos.compareCommitsWithBasehead = async () => {throw new Error('API failure');};
  await assert.rejects(f.run(), /API failure/); assert.equal(f.approvals.length, 0);
});

test('head, base, author or file-count changes during inspection fail closed', async () => {
  for (const mutate of [f => {f.pr.head.sha = 'c'.repeat(40);}, f => {f.pr.base.sha = 'c'.repeat(40);},
    f => {f.pr.user.login = 'someone';}, f => {f.pr.changed_files++;}, f => {f.pr.draft = true;}]) {
    const f = fixture(); const original = f.github.rest.pulls.get; let reads = 0;
    f.github.rest.pulls.get = async () => {if (++reads === 2) mutate(f); return original();};
    await assert.rejects(f.run(), /PR changed during inspection/); assert.equal(f.approvals.length, 0);
  }
});

test('duplicate events do not create duplicate approvals; dismissed ones are renewed', async () => {
  const f = fixture(); await f.run();
  f.reviews.push({user: {login: 'antfly-docs-approver[bot]'}, state: 'APPROVED',
    commit_id: f.pr.head.sha, body: f.approvals[0].body});
  await f.run(); assert.equal(f.approvals.length, 1);
  f.reviews[0].state = 'DISMISSED'; await f.run(); assert.equal(f.approvals.length, 2);
});

test('unexpected repository or event cannot approve', async () => {
  for (const mutate of [f => {f.context.repo.owner = 'other';}, f => {f.context.eventName = 'pull_request';}]) {
    const f = fixture(); mutate(f); await assert.rejects(f.run(), /Unexpected/);
    assert.equal(f.approvals.length, 0);
  }
});

test('privileged workflow uses trusted base checkout and executes no PR commands', () => {
  const workflow = fs.readFileSync('.github/workflows/docs-design-approval.yml', 'utf8');
  assert.match(workflow, /pull_request_target:/);
  assert.match(workflow, /ref: \$\{\{ github.sha \}\}/);
  assert.match(workflow, /persist-credentials: false/);
  assert.match(workflow, /sparse-checkout: \.github\/scripts/);
  assert.match(workflow, /^    environment: docs-design-approval$/m);
  assert.match(workflow, /^      pull-requests: read$/m);
  assert.doesNotMatch(workflow, /^\s+pull-requests: write$/m);
  assert.match(workflow, /private-key: \$\{\{ secrets.DOCS_APPROVAL_APP_PRIVATE_KEY \}\}/);
  assert.match(workflow, /repositories: antfly/);
  assert.match(workflow, /permission-contents: write/);
  assert.match(workflow, /^      contents: read$/m);
  assert.match(workflow, /permission-pull-requests: write/);
  assert.match(workflow, /github-token: \$\{\{ steps.approval-app.outputs.token \}\}/);
  for (const [, action] of workflow.matchAll(/uses: (\S+)/g)) {
    assert.match(action, /^actions\/(checkout|github-script|create-github-app-token)@[a-f0-9]{40}$/);
  }
  assert.doesNotMatch(workflow, /head\.sha|\brun:/);
});

test('default Actions token or missing reviewer identity cannot submit approvals', async () => {
  const f = fixture();
  for (const reviewerLogin of [undefined, '', 'github-actions[bot]', 'ajroetker', '[bot]']) {
    await assert.rejects(main({github: f.github, context: f.context, core: {}, reviewerLogin}),
      /dedicated GitHub App reviewer/);
  }
  assert.equal(f.approvals.length, 0);
});

test('another bot or legacy Actions approval does not suppress the dedicated App review', async () => {
  for (const login of ['github-actions[bot]', 'another-app[bot]']) {
    const f = fixture(); await f.run();
    f.reviews.push({user: {login}, state: 'APPROVED', commit_id: f.pr.head.sha, body: f.approvals[0].body});
    await f.run(); assert.equal(f.approvals.length, 2);
  }
});

test('configuration and runtime-only PRs require human review', async () => {
  for (const filename of ['ts/packages/design-system/package.json',
    'ts/packages/design-system/vite.config.ts', 'zig/pkg/antfly/src/main.zig']) {
    const f = fixture(); f.files[0].filename = filename;
    for (const tree of f.trees) tree.tree[0].path = filename;
    await f.run(); assert.equal(f.approvals.length, 0, filename);
  }
});


test('immutable SHA comparison rejects the A -> B -> A file-list substitution', async () => {
  const f = fixture();
  const unsafeHead = f.pr.head.sha;
  const base = f.pr.base.sha;
  const temporaryHead = 'c'.repeat(40);
  const unsafePath = 'backend/dangerous-change.go';
  f.comparison.files = [{filename: unsafePath, status: 'modified'}];
  for (const tree of f.trees) tree.tree.push({path: unsafePath, type: 'blob', mode: '100644'});
  const original = f.github.rest.repos.compareCommitsWithBasehead;
  let mutableFileReads = 0;
  f.github.rest.pulls.listFiles = async () => {
    mutableFileReads++;
    f.pr.head.sha = temporaryHead;
    try {
      return [{filename: 'docs/guide.md', status: 'modified'}];
    } finally {
      f.pr.head.sha = unsafeHead;
    }
  };
  f.github.rest.repos.compareCommitsWithBasehead = async args => {
    // Both PR snapshots show A, while the live branch briefly points to B.
    f.pr.head.sha = temporaryHead;
    try {
      assert.equal(args.basehead, `${base}...${unsafeHead}`);
      return await original(args);
    } finally {
      f.pr.head.sha = unsafeHead;
    }
  };
  await f.run();
  assert.equal(f.approvals.length, 0);
  assert.equal(mutableFileReads, 0);
});

test('comparison is pinned to both snapshot SHAs and uses page one', async () => {
  const f = fixture(); await f.run();
  assert.equal(f.comparisons.length, 1);
  assert.equal(f.comparisons[0].basehead, `${f.pr.base.sha}...${f.pr.head.sha}`);
  assert.equal(f.comparisons[0].page, 1);
  assert.equal(f.approvals.length, 1);
});

test('missing, mismatched or truncated comparisons never approve', async () => {
  for (const mutate of [
    f => {f.comparison.base_commit.sha = 'd'.repeat(40);},
    f => {delete f.comparison.base_commit;},
    f => {delete f.comparison.merge_base_commit;},
    f => {f.comparison.merge_base_commit.sha = 'main';},
    f => {delete f.comparison.files;},
    f => {f.comparison.truncated = true;},
    f => {f.comparison.files = Array.from({length: 300}, () => ({filename: 'docs/guide.md', status: 'modified'}));},
    f => {f.pr.changed_files = 2;},
  ]) {
    const f = fixture(); mutate(f);
    await f.run().catch(error => assert.match(error.message, /immutable commit comparison/));
    assert.equal(f.approvals.length, 0);
  }
});

test('299-file comparisons can approve, 300-file comparisons require review', async () => {
  for (const count of [299, 300]) {
    const f = fixture(); f.pr.changed_files = count;
    f.comparison.files = Array.from({length: count}, (_, i) => ({filename: `docs/page-${i}.md`, status: 'added'}));
    f.trees[1].tree = f.comparison.files.map(file => ({path: file.filename, mode: '100644', type: 'blob'}));
    await f.run().catch(error => assert.match(error.message, /immutable commit comparison/));
    assert.equal(f.approvals.length, count < 300 ? 1 : 0);
  }
});

test('rename source is checked in the immutable merge-base tree', async () => {
  const f = fixture();
  f.comparison.merge_base_commit.sha = 'e'.repeat(40);
  f.files[0].status = 'renamed'; f.files[0].previous_filename = 'docs/old.md';
  f.trees[0].tree[0].path = 'docs/old.md';
  await f.run();
  assert.deepEqual(f.treeReads, [f.comparison.merge_base_commit.sha, f.pr.head.sha]);
  assert.equal(f.approvals.length, 1);
});


test('large or unavailable change counts require human review without comparison or approval', async () => {
  for (const count of [300, 649, 3000, 0, undefined, NaN]) {
    const f = fixture(); f.pr.changed_files = count;
    await f.run();
    assert.equal(f.approvals.length, 0);
    assert.equal(f.comparisons.length, 0);
    assert.equal(f.treeReads.length, 0);
    assert.equal(f.getReads(), 1);
    assert.match(f.notices.at(-1), /Human review required/);
  }
});
