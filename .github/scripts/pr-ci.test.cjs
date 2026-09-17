'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {main, route, selectSuites, validateSnapshot} = require('./pr-ci.cjs');
const config = require('./pr-ci-config.json');
const SHA = 'a'.repeat(40);
const BASE = 'b'.repeat(40);

function fixture() {
  const repository = {full_name: 'acme/project', default_branch: 'main'};
  const pr = {number: 7, state: 'open', draft: false, labels: [],
    head: {sha: SHA, repo: repository}, base: {sha: BASE, ref: 'main', repo: repository}};
  const comment = {id: 17, user: {login: 'maintainer', type: 'User'},
    body: `/ci run ${SHA}`, created_at: '2026-09-15T01:00:00Z', updated_at: '2026-09-15T01:00:00Z',
    issue_url: 'https://api.github.com/repos/acme/project/issues/7'};
  const context = {repo: {owner: 'acme', repo: 'project'}, eventName: 'issue_comment',
    ref: 'refs/heads/main', runId: 91, apiUrl: 'https://api.github.com',
    payload: {action: 'created', repository, issue: {number: 7, pull_request: {}, labels: []}, comment}};
  const statuses = [];
  const checks = [], dispatches = [], cancelled = [], runs = [], outputs = {}, notices = [];
  let permission = 'write';
  let files = [{filename: 'docs/guide.md'}];
  let jobs = [{name: 'PR CI result', conclusion: 'success'}];
  const github = {rest: {
    pulls: {get: async () => ({data: structuredClone(pr)}), listFiles: async () => files},
    checks: {
      get: async ({check_run_id}) => ({data: structuredClone(checks.find(c => c.id === check_run_id))}),
      listForRef: async ({ref}) => checks.filter(c => c.head_sha === ref).slice().reverse(),
      create: async body => {
        const c = {...structuredClone(body), id: checks.length + 1, app: {slug: 'github-actions'}};
        checks.push(c);
        return {data: structuredClone(c)};
      },
      update: async body => {
        const c = checks.find(c => c.id === body.check_run_id);
        Object.assign(c, structuredClone(body));
        // GitHub retains a previous conclusion when status alone is updated.
        if (c.conclusion) c.status = 'completed';
        return {data: structuredClone(c)};
      },
    },
    issues: {getComment: async () => ({data: structuredClone(comment)})},
    repos: {createCommitStatus: async body => {statuses.push(structuredClone(body));}, getCollaboratorPermissionLevel: async () => {
      if (typeof permission === 'number') throw Object.assign(new Error('Permission lookup failed'), {status: permission});
      return {data: {permission}};
    }},
    actions: {
      listWorkflowRuns: async ({status}) => runs.filter(r => r.status === status),
      cancelWorkflowRun: async ({run_id}) => {cancelled.push(run_id);},
      createWorkflowDispatch: async data => {dispatches.push(data);},
      listJobsForWorkflowRun: async () => jobs,
      getWorkflowRun: async () => ({data: structuredClone(context.payload.workflow_run)}),
    },
  }, paginate: async (method, args) => method(args)};
  const core = {setOutput: (k,v) => {outputs[k]=v;}, notice: msg => notices.push(msg)};
  const env = {PR_NUMBER: '7', CHECK_ID: '1', COMMENT_ID: '17', HEAD_SHA: SHA, BASE_SHA: BASE};
  return {pr, comment, context, checks, statuses, notices, dispatches, cancelled, runs, outputs, env, github,
    permission: value => {permission = value;}, files: value => {files = value;},
    jobs: value => {jobs = value;},
    call: (mode='event') => main({github, context, core, mode, config, env}),
    finish: () => {
      context.eventName = 'workflow_run';
      context.payload.workflow_run = {id: 91, name: 'PR CI #7 / check 1 / approval 17',
        display_title: 'PR CI #7 / check 1 / approval 17', run_attempt: 1,
        path: '.github/workflows/pr-ci.yml', event: 'workflow_dispatch', head_branch: 'main', conclusion: 'success'};
    },
  };
}

test('drafts, closed PRs, foreign targets, missing heads, bots, readers, and stale SHAs never dispatch', async t => {
  for (const change of [
    f => {f.pr.draft=true;}, f => {f.pr.state='closed';},
    f => {f.pr.base.repo={full_name:'other/project'};},
    f => {f.pr.head.repo=null;},
    f => {f.comment.user.type='Bot';}, f => {f.permission('read');},
    f => {f.comment.body='/ci run '+BASE;},
    f => {f.permission('triage');},
    f => {f.permission('none');},
    f => {f.permission(403);},
    f => {f.permission(500);},
    f => {f.comment.updated_at='2026-09-16T01:00:00Z';},
  ]) await t.test(change.toString(), async () => {
    const f=fixture(); change(f); await f.call();
    assert.equal(f.dispatches.length,0);
    assert.equal(f.cancelled.length,0);
  });
});

test('approval is consumed once, uses the default branch, and publishes on the PR head', async () => {
  const f=fixture(); await f.call();
  assert.equal(f.dispatches[0].ref,'main');
  assert.equal(f.checks[0].head_sha,SHA);
  assert.equal(f.checks[0].status,'queued');
  await f.call(); assert.equal(f.dispatches.length,1); // duplicate webhook
  await f.call('admit'); assert.equal(f.checks[0].status,'in_progress');
  assert.equal(f.outputs.head_sha,SHA);
  await assert.rejects(f.call('admit'),/already used/);
  f.finish(); await f.call();
  assert.equal(f.checks[0].conclusion,'success');
  await assert.rejects(f.call('verify'),/expired/);
});

test('PR checks link to the admitted run through completion and revocation', async t => {
  for (const outcome of ['success', 'failure', 'cancelled', 'revoked']) {
    await t.test(outcome, async () => {
      const f = fixture();
      await f.call();
      assert.equal(f.checks[0].details_url, undefined);
      assert.ok(!f.checks[0].output.summary.includes('View CI run'));
      await f.call('admit');
      const url = 'https://github.com/acme/project/actions/runs/91';
      const assertLink = () => {
        assert.equal(f.checks[0].details_url, url);
        assert.ok(f.checks[0].output.summary.endsWith(`[View CI run](${url})`));
        assert.equal(JSON.parse(f.checks[0].output.text).run_id, 91);
      };
      assertLink();
      if (outcome === 'revoked') {
        f.context.payload.action = 'edited';
      } else {
        f.finish();
        f.context.payload.workflow_run.conclusion = outcome;
      }
      await f.call();
      assert.equal(f.checks[0].status, 'completed');
      assertLink();
    });
  }
});

test('run links use the configured GitHub server', async () => {
  const f = fixture();
  f.context.serverUrl = 'https://github.example.com';
  await f.call();
  await f.call('admit');
  assert.equal(f.checks[0].details_url, 'https://github.example.com/acme/project/actions/runs/91');
});

test('writers, maintainers, and admins approve using the default token, including outside collaborators', async t => {
  for (const permission of ['write','maintain','admin']) await t.test(permission,async()=>{
    const f=fixture(); f.permission(permission);
    f.comment.author_association='COLLABORATOR';
    await f.call(); await f.call('admit'); await f.call('verify');
    f.finish(); await f.call();
    assert.equal(f.dispatches.length,1);
    assert.equal(f.checks[0].conclusion,'success');
    assert.equal(f.github.rest.orgs,undefined);
    assert.ok(Object.keys(f.env).every(key=>!key.includes('TOKEN')));
  });
});

test('organization membership does not authorize a repository reader', async () => {
  const f=fixture(); f.comment.author_association='MEMBER'; f.permission('read');
  await f.call(); assert.equal(f.dispatches.length,0);
});

test('permission revocation prevents run and suite admission', async () => {
  const f=fixture(); await f.call(); f.permission('read');
  await assert.rejects(f.call('admit'),/write access/);
  f.permission('write'); await f.call('admit'); f.permission('read');
  await assert.rejects(f.call('verify'),/write access/);
});

test('suites are a snapshot and optional labels never dispatch by themselves', async () => {
  const f=fixture(); const optional=config.suites.find(s=>s.label);
  f.pr.labels=[{name:optional.label}];
  f.context.payload.issue.labels=structuredClone(f.pr.labels);
  f.context.eventName='pull_request_target';
  f.context.payload.action='labeled'; f.context.payload.label={name:optional.label};
  await f.call(); assert.equal(f.dispatches.length,0);
  f.context.eventName='issue_comment'; f.context.payload.action='created';
  await f.call(); f.env.CHECK_ID=f.dispatches[0].inputs.check_id; await f.call('admit');
  assert.ok(JSON.parse(f.outputs.suites).includes(optional.id));
  f.pr.labels=[];
  await assert.rejects(f.call('verify'),/selected suites changed/);
});

test('new head, new base, draft conversion, and removed labels reject stale admission', async t => {
  for (const change of [f=>{f.pr.head.sha=BASE;},f=>{f.pr.base.sha=SHA;},
    f=>{f.pr.base.ref='release';}, f=>{f.pr.draft=true;},
    f=>{f.pr.labels=[{name:config.suites.find(s=>s.label).label}];}]) {
    await t.test(change.toString(),async()=>{
      const f=fixture(); await f.call(); change(f);
      await assert.rejects(f.call('admit'));
      assert.equal(f.checks[0].status,'queued');
    });
  }
});

test('admission rejects fabricated records, other refs, changed checkout, and reruns', async t => {
  for (const change of [
    f=>{f.checks[0].app.slug='another-app';},
    f=>{f.env.COMMENT_ID='999';},
    f=>{f.context.ref='refs/heads/untrusted';},
    f=>{f.env.GITHUB_RUN_ATTEMPT='2';},
    f=>{f.checks[0].head_sha=BASE;},
  ]) await t.test(change.toString(),async()=>{
    const f=fixture(); await f.call(); change(f); await assert.rejects(f.call('admit'));
  });
  const f=fixture(); await f.call(); await f.call('admit');
  f.env.HEAD_SHA=BASE;
  await assert.rejects(f.call('verify'),/checkout/);
  f.env.HEAD_SHA=SHA; f.env.SUITE='unapproved';
  await assert.rejects(f.call('verify'),/Suite was not approved/);
});

test('a fresh approval revokes the old run; a late completion cannot pass it', async () => {
  const f=fixture(); await f.call(); await f.call('admit');
  f.runs.push({id:91,status:'in_progress',display_title:'PR CI #7 / check 1 / approval 17'});
  f.comment.id=18; f.env.COMMENT_ID='18'; await f.call();
  assert.deepEqual(f.cancelled,[91]);
  assert.equal(f.checks[0].conclusion,'action_required');
  assert.equal(f.checks[1].status,'queued');
  assert.equal(f.checks[1].details_url, undefined);
  assert.equal(f.statuses.at(-1).state, 'pending');
  assert.ok(f.statuses.at(-1).target_url.includes('/actions/workflows/'));
  assert.ok(!f.checks[1].output.summary.includes('View CI run'));
  f.finish(); await f.call();
  assert.equal(f.checks[1].status,'queued');
  assert.equal(JSON.parse(f.checks[1].output.text).comment_id,18);
});

test('unauthorized comments do not cancel legitimate runs', async () => {
  const f=fixture(); await f.call(); await f.call('admit');
  f.comment.id=18; f.permission('read'); await f.call();
  assert.equal(f.checks[0].status,'in_progress'); assert.equal(f.cancelled.length,0);
});

test('labels added after the comment do not buy extra tests', async () => {
  const f=fixture(); f.pr.labels=[{name:config.suites.find(s=>s.label).label}];
  await f.call(); assert.equal(f.dispatches.length,0);
});

test('invalidating approval does not allow its webhook to be replayed', async () => {
  const f=fixture(); await f.call();
  f.context.eventName='pull_request_target'; f.context.payload.action='ready_for_review';
  await f.call();
  f.context.eventName='issue_comment'; f.context.payload.action='created';
  await f.call(); assert.equal(f.dispatches.length,1);
  assert.equal(f.checks[0].conclusion,'action_required');
});

test('PR updates cancel old compute and mark the current head as needing approval', async () => {
  const f=fixture(); await f.call(); await f.call('admit');
  f.runs.push({id:91,status:'in_progress',display_title:'PR CI #7 / check 1 / approval 17'});
  f.pr.head.sha=BASE; f.context.eventName='pull_request_target'; f.context.payload.action='synchronize';
  await f.call();
  assert.deepEqual(f.cancelled,[91]);
  assert.equal(f.checks[0].conclusion,'action_required');
  assert.equal(f.checks[1].head_sha,BASE);
  assert.equal(f.checks[1].conclusion,'action_required');
  f.finish(); await f.call();
  assert.equal(f.checks[0].conclusion,'action_required');
});

test('editing or deleting an approval invalidates it even after a successful run', async () => {
  for (const action of ['edited','deleted']) {
    const f=fixture(); await f.call(); await f.call('admit'); f.finish(); await f.call();
    f.context.eventName='issue_comment'; f.context.payload.action=action;
    f.comment.body='removed approval'; await f.call();
    assert.equal(f.checks[0].conclusion,'action_required');
  }
});

test('failed/cancelled/skipped CI, missing result, changed approval and changed head fail closed', async t => {
  for (const change of [
    f=>{f.context.payload.workflow_run.conclusion='failure';},
    f=>{f.context.payload.workflow_run.conclusion='cancelled';},
    f=>{f.context.payload.workflow_run.conclusion='skipped';},
    f=>{f.jobs([]);}, f=>{f.pr.head.sha=BASE;},
    f=>{f.comment.body='changed';}, f=>{f.permission('read');},
    f=>{f.context.payload.workflow_run.run_attempt=2;},
  ]) await t.test(change.toString(),async()=>{
    const f=fixture(); await f.call(); await f.call('admit'); f.finish(); change(f); await f.call();
    assert.equal(f.checks[0].conclusion,'failure');
  });
});

test('a workflow that never admitted approval cannot pass the check', async () => {
  const f=fixture(); await f.call(); f.finish(); await f.call();
  assert.equal(f.checks[0].conclusion,'failure');
});

test('large diffs select all standard suites; renamed paths are considered', async () => {
  const f=fixture(); f.files(Array.from({length:3000},()=>({filename:'docs/file.md'})));
  await f.call();
  assert.deepEqual(JSON.parse(f.checks[0].output.text).suites,config.suites.filter(s=>!s.label).map(s=>s.id));
  const g=fixture();
  const target=config.suites.find(s=>s.paths);
  const filename=target.id==='proxy'?'go/pkg/proxy/old.go':'go/pkg/old.go';
  g.files([{filename:'docs/moved.txt',previous_filename:filename}]); await g.call();
  assert.ok(JSON.parse(g.checks[0].output.text).suites.includes(target.id));
});

test('path selection matches repository CI owners and does not enable optional tests', () => {
  const f=fixture();
  if (config.suites.some(s=>s.id==='zig')) {
    assert.deepEqual(selectSuites(['go/pkg/proxy/test.go'],f.pr,config),['policy','zig','sdks','proxy']);
    assert.ok(selectSuites(['specs/openapi/new.yaml'],f.pr,config).includes('operator'));
    assert.ok(selectSuites(['go/pkg/operator/api/types.go'],f.pr,config).includes('operator'));
    assert.ok(!selectSuites(['go/pkg/operator/work-log/README.md'],f.pr,config).includes('operator'));
    assert.ok(!selectSuites(['go/pkg/proxy/docs/notes.mdx'],f.pr,config).includes('proxy'));
  } else {
    assert.deepEqual(selectSuites(['infra/src/test.go'],f.pr,config),['policy','infra']);
    assert.deepEqual(selectSuites(['ts/apps/dashboard/test.ts'],f.pr,config),['policy','vitest','playwright']);
    assert.ok(selectSuites(['ts/apps/www-antfly/app/page.tsx'],f.pr,config).includes('www'));
    assert.deepEqual(selectSuites(['README.md'],f.pr,config),['policy']);
  }
});

test('completion routing uses the orchestrator title, not a branch or arbitrary workflow', () => {
  const f=fixture(); f.finish(); assert.equal(route(f.context),'7');
  f.context.payload.workflow_run.path='.github/workflows/release.yml'; assert.equal(route(f.context),'');
});

test('every expensive worker is gated, pins its checkout, and disables automatic PR triggers', () => {
  const root=path.resolve(__dirname,'../workflows');
  for (const suite of config.suites) {
    const text=fs.readFileSync(path.join(root,suite.workflow),'utf8');
    assert.doesNotMatch(text,/^  pull_request(?:_target)?:/m,suite.workflow);
    assert.match(text,/uses: \.\/\.github\/workflows\/pr-ci-admission.yml/);
    const blocks=text.split(/^  [\w-]+:\n/m).filter(s=>/^    runs-on:/m.test(s));
    for (const block of blocks) {
      assert.match(block,/needs\.admission\.result == 'success'/,suite.workflow);
      assert.match(block,/!cancelled\(\)/,suite.workflow);
      assert.match(block,/github\.run_attempt == 1/,suite.workflow);
    }
    const checkouts=text.match(/uses: actions\/checkout@[^\n]+\n[\s\S]*?(?=\n      -|$)/g)||[];
    for (const checkout of checkouts) {
      if (suite.id === 'policy' && /path: trusted-ci\n/.test(checkout)) {
        assert.match(checkout,/ref: \$\{\{ github.workflow_sha \}\}/);
      } else {
        assert.match(checkout,/ref: \$\{\{ inputs.head_sha \|\| github.sha \}\}/);
      }
      assert.match(checkout,/persist-credentials: false/);
    }
  }
  for (const file of fs.readdirSync(root)) {
    const text=fs.readFileSync(path.join(root,file),'utf8');
    if (file!=='pr-ci-controller.yml') assert.doesNotMatch(text,/^  pull_request(?:_target)?:/m,file);
    if (file.startsWith('pr-ci')) {
      assert.doesNotMatch(text,/PR_CI_MEMBERS_TOKEN|PR_CI_APPROVERS|create-github-app-token/,file);
      assert.doesNotMatch(text,/secrets: inherit/,file);
    }
  }
});

test('policy validates the executing workflow even when a release predates the controller', () => {
  const text=fs.readFileSync(path.resolve(__dirname,'../workflows/pr-ci-policy.yml'),'utf8');
  assert.match(text,/ref: \$\{\{ github.workflow_sha \}\}\n\s+path: trusted-ci/);
  assert.match(text,/name: Test executing CI policy\n\s+working-directory: trusted-ci\n\s+run: node --test \.github\/scripts\/pr-ci.test.cjs/);
  assert.match(text,/name: Test proposed CI policy when present\n\s+if: \$\{\{ hashFiles\('\.github\/scripts\/pr-ci.test.cjs'\) != '' \}\}\n\s+run: node --test \.github\/scripts\/pr-ci.test.cjs/);
});

// The live rollout first creates an action_required check before an approval.
test('approval after waiting or successful CI creates a fresh consumable check', async () => {
  const f=fixture();
  f.context.eventName='pull_request_target'; f.context.payload.action='opened';
  await f.call(); assert.equal(f.checks[0].conclusion,'action_required');
  f.context.eventName='issue_comment'; f.context.payload.action='created';
  await f.call();
  assert.equal(f.dispatches[0].inputs.check_id,'2');
  assert.equal(f.checks[1].status,'queued');
  f.env.CHECK_ID='2'; await f.call('admit'); await f.call('verify');
  f.finish(); f.context.payload.workflow_run.display_title='PR CI #7 / check 2 / approval 17';
  await f.call(); assert.equal(f.checks[1].conclusion,'success');
  f.context.eventName='issue_comment'; f.comment.id=18; f.env.COMMENT_ID='18';
  await f.call(); assert.equal(f.dispatches[1].inputs.check_id,'3');
  f.env.CHECK_ID='3'; await f.call('admit'); await f.call('verify');
  await f.call(); assert.equal(f.dispatches.length,2);
});

test('trusted finalizer publishes without a workflow_run event and rechecks the result', async () => {
  for (const success of [true, false]) {
    const f=fixture(); await f.call(); await f.call('admit');
    f.finish(); f.context.eventName='workflow_dispatch';
    f.context.payload.workflow_run.conclusion=null;
    f.env.RESULT=success?'success':'failure';
    await f.call('complete');
    assert.equal(f.checks[0].conclusion,success?'success':'failure');
  }
  const f=fixture(); await f.call(); await f.call('admit');
  f.finish(); f.context.eventName='workflow_dispatch'; f.env.RESULT='success';
  f.jobs([]); await f.call('complete');
  assert.equal(f.checks[0].conclusion,'failure');
});

test('finalizer rejects other approval inputs and cannot restore a revoked check', async () => {
  const f=fixture(); await f.call(); await f.call('admit'); f.finish();
  f.context.eventName='workflow_dispatch'; f.env.RESULT='success'; f.env.CHECK_ID='2';
  await assert.rejects(f.call('complete'),/does not match/);
  f.env.CHECK_ID='1';
  f.checks[0].output.text=JSON.stringify({...JSON.parse(f.checks[0].output.text),revoked:true});
  await f.call('complete'); assert.notEqual(f.checks[0].conclusion,'success');
});


test('PR CI gate status links queued approval to listing and admitted run to jobs', async () => {
  const f = fixture();
  await f.call();
  assert.deepEqual(f.statuses.at(-1), {
    owner: 'acme', repo: 'project', sha: SHA, context: 'PR CI gate', state: 'pending',
    target_url: 'https://github.com/acme/project/actions/workflows/pr-ci.yml?query=PR%20CI%20%237%20%2F',
    description: 'CI queued; view workflow runs',
  });
  await f.call('admit');
  assert.equal(f.statuses.at(-1).state, 'pending');
  assert.equal(f.statuses.at(-1).target_url, 'https://github.com/acme/project/actions/runs/91');
  f.finish(); await f.call();
  assert.equal(f.statuses.at(-1).state, 'success');
  assert.equal(f.statuses.at(-1).sha, SHA);
});

test('PR CI gate status reflects failure, dispatch failure, and invalidated approval', async t => {
  for (const outcome of ['failure', 'dispatch failure', 'revoked']) await t.test(outcome, async () => {
    const f = fixture();
    if (outcome === 'dispatch failure') {
      f.github.rest.actions.createWorkflowDispatch = async () => {throw new Error('dispatch failed');};
      await assert.rejects(f.call(), /dispatch failed/);
    } else {
      await f.call(); await f.call('admit');
      if (outcome === 'revoked') {
        f.context.payload.action = 'edited';
      } else {
        f.finish(); f.context.payload.workflow_run.conclusion = 'failure';
      }
      await f.call();
    }
    assert.equal(f.statuses.at(-1).state, outcome === 'revoked' ? 'error' : 'failure');
    assert.notEqual(f.checks.at(-1).conclusion, 'success');
  });
});

test('PR CI gate supports enterprise URLs', async () => {
  const f = fixture(); f.context.serverUrl = 'https://github.example.com';
  await f.call();
  assert.ok(f.statuses.at(-1).target_url.startsWith('https://github.example.com/'));
  await f.call('admit');
  assert.equal(f.statuses.at(-1).target_url, 'https://github.example.com/acme/project/actions/runs/91');
});

test('gate publication failure prevents dispatch and approval consumption', async () => {
  const f = fixture();
  f.github.rest.repos.createCommitStatus = async () => {throw new Error('permission denied');};
  await assert.rejects(f.call(), /permission denied/);
  assert.equal(f.dispatches.length, 0);
  assert.equal(f.checks.at(-1).status, 'queued');
  await assert.rejects(f.call('admit'), /permission denied/);
  assert.equal(JSON.parse(f.checks.at(-1).output.text).run_id, undefined);
});

test('gate publication failure cannot publish a successful check', async () => {
  const f = fixture(); await f.call(); await f.call('admit'); f.finish();
  f.github.rest.repos.createCommitStatus = async () => {throw new Error('status unavailable');};
  await assert.rejects(f.call(), /status unavailable/);
  assert.equal(f.statuses.at(-1).state, 'pending');
  assert.notEqual(f.checks.at(-1).conclusion, 'success');
});

test('gate invalidation replaces success on the same head', async () => {
  const f = fixture(); await f.call(); await f.call('admit'); f.finish(); await f.call();
  assert.equal(f.statuses.at(-1).state, 'success');
  f.context.eventName = 'pull_request_target';
  f.context.payload.action = 'converted_to_draft'; f.pr.draft = true;
  await f.call();
  assert.equal(f.statuses.at(-1).context, 'PR CI gate');
  assert.equal(f.statuses.at(-1).sha, SHA);
  assert.equal(f.statuses.at(-1).state, 'error');
});


test('dispatch returns an exact queued link without consuming admission', async () => {
  const f = fixture();
  f.github.rest.actions.createWorkflowDispatch = async body => {
    f.dispatches.push(body);
    return {data: {workflow_run_id: 91}};
  };
  await f.call();
  assert.equal(f.dispatches[0].return_run_details, true);
  assert.equal(f.statuses.at(-1).state, 'pending');
  assert.equal(f.statuses.at(-1).target_url, 'https://github.com/acme/project/actions/runs/91');
  assert.equal(f.statuses.at(-1).description, 'CI queued; view workflow runs');
  assert.equal(f.checks.at(-1).status, 'queued');
  assert.equal(JSON.parse(f.checks.at(-1).output.text).run_id, undefined);
  assert.equal(JSON.parse(f.checks.at(-1).output.text).dispatched_run_id, 91);
  await f.call(); assert.equal(f.dispatches.length, 1);
  f.context.runId = 92;
  await assert.rejects(f.call('admit'), /another dispatched run/);
  f.context.runId = 91;
  await f.call('admit');
  assert.equal(f.checks.at(-1).status, 'in_progress');
  f.finish(); await f.call();
  assert.equal(f.statuses.at(-1).state, 'success');
});

test('queued dispatch cannot pass without admission or finish from another run', async () => {
  const f = fixture();
  f.github.rest.actions.createWorkflowDispatch = async () => ({data: {workflow_run_id: 91}});
  await f.call(); f.finish(); f.context.payload.workflow_run.id = 92;
  await f.call();
  assert.equal(f.statuses.at(-1).state, 'pending');
  f.context.payload.workflow_run.id = 91;
  await f.call();
  assert.equal(f.statuses.at(-1).state, 'failure');
});

test('invalid dispatch IDs fail closed', async () => {
  for (const id of [null, 0, -1, '91', 1.5]) {
    const f = fixture();
    f.github.rest.actions.createWorkflowDispatch = async () => ({data: {workflow_run_id: id}});
    await assert.rejects(f.call(), /Invalid dispatched run ID/);
    assert.equal(f.statuses.at(-1).state, 'failure');
  }
});

test('queued run link is replaced on a fresh approval', async () => {
  const f = fixture();
  let id = 91;
  f.github.rest.actions.createWorkflowDispatch = async () => ({data: {workflow_run_id: id}});
  await f.call();
  f.comment.id = 18; id = 92;
  await f.call();
  assert.equal(f.statuses.at(-1).target_url, 'https://github.com/acme/project/actions/runs/92');
  assert.equal(f.checks.at(-1).status, 'queued');
});


test('maintainer-approved fork PRs complete using the base repository and exact head SHA', async () => {
  const f = fixture();
  f.pr.head.repo = {full_name: 'contributor/project'};
  await f.call();
  assert.equal(f.dispatches.length, 1);
  assert.equal(f.dispatches[0].owner, 'acme');
  assert.equal(f.dispatches[0].repo, 'project');
  assert.equal(f.dispatches[0].ref, 'main');
  await f.call('admit');
  assert.equal(f.outputs.head_sha, SHA);
  await f.call('verify');
  f.finish(); await f.call();
  assert.equal(f.checks[0].conclusion, 'success');
  assert.equal(f.statuses.at(-1).sha, SHA);
  assert.equal(f.statuses.at(-1).repo, 'project');
});

test('fork authors without upstream write access cannot approve CI', async () => {
  const f = fixture();
  f.pr.head.repo = {full_name: 'contributor/project'};
  f.comment.user.login = 'contributor';
  f.permission('read');
  await f.call();
  assert.equal(f.dispatches.length, 0);
  assert.match(f.notices[0], /write access/);
});

test('fork pushes invalidate approval at admission, verification, and completion', async t => {
  for (const mode of ['admit', 'verify', 'complete']) await t.test(mode, async () => {
    const f = fixture();
    f.pr.head.repo = {full_name: 'contributor/project'};
    await f.call();
    if (mode !== 'admit') await f.call('admit');
    f.pr.head.sha = 'c'.repeat(40);
    if (mode === 'complete') {
      f.finish(); await f.call();
      assert.equal(f.checks[0].conclusion, 'failure');
    } else {
      await assert.rejects(f.call(mode), /Commit, base, or selected suites changed/);
    }
  });
});


test('PR orchestrator limits all called suites to read-only GitHub caches', () => {
  const root = path.resolve(__dirname, '../workflows');
  const orchestrator = fs.readFileSync(path.join(root, 'pr-ci.yml'), 'utf8');
  assert.match(orchestrator, /^cache-mode: read$/m);
  // A calling job could override the top-level limit; forbid broader access.
  for (const file of ['pr-ci.yml', 'pr-ci-admission.yml', ...config.suites.map(s => s.workflow)]) {
    const workflow = fs.readFileSync(path.join(root, file), 'utf8');
    for (const match of workflow.matchAll(/^\s*cache-mode:\s*(.*?)\s*$/gm)) {
      assert.ok(['read', 'none'].includes(match[1]), `${file} broadens PR cache access`);
    }
  }
});
