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
  const checks = [], dispatches = [], cancelled = [], runs = [], outputs = {}, notices = [];
  let membership = 'active';
  const membershipRequests = [];
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
        if (body.status !== 'completed') c.conclusion = null;
        return {data: structuredClone(c)};
      },
    },
    issues: {getComment: async () => ({data: structuredClone(comment)})},
    orgs: {getMembershipForUser: async args => {
      membershipRequests.push(args);
      if (typeof membership === 'number') throw Object.assign(new Error('Membership lookup failed'), {status: membership});
      return {data: {state: membership}};
    }},
    actions: {
      listWorkflowRuns: async ({status}) => runs.filter(r => r.status === status),
      cancelWorkflowRun: async ({run_id}) => {cancelled.push(run_id);},
      createWorkflowDispatch: async data => {dispatches.push(data);},
      listJobsForWorkflowRun: async () => jobs,
    },
  }, paginate: async (method, args) => method(args)};
  const core = {setOutput: (k,v) => {outputs[k]=v;}, notice: msg => notices.push(msg)};
  const env = {PR_NUMBER: '7', CHECK_ID: '1', COMMENT_ID: '17', HEAD_SHA: SHA, BASE_SHA: BASE, PR_CI_MEMBERS_TOKEN: 'test-members-read-token'};
  return {pr, comment, context, checks, dispatches, cancelled, runs, outputs, env, github, membershipRequests,
    membership: value => {membership = value;}, files: value => {files = value;},
    jobs: value => {jobs = value;},
    call: (mode='event') => main({github, context, core, mode, config, env}),
    finish: () => {
      context.eventName = 'workflow_run';
      context.payload.workflow_run = {id: 91, name: 'Approved PR CI',
        display_title: 'PR CI #7 / check 1 / approval 17', run_attempt: 1,
        path: '.github/workflows/pr-ci.yml', event: 'workflow_dispatch', head_branch: 'main', conclusion: 'success'};
    },
  };
}

test('drafts, closed PRs, forks, bots, nonmembers, and stale SHAs never dispatch', async t => {
  for (const change of [
    f => {f.pr.draft=true;}, f => {f.pr.state='closed';},
    f => {f.pr.head.repo={full_name:'fork/project'};},
    f => {f.comment.user.type='Bot';}, f => {f.membership(404);},
    f => {f.comment.body='/ci run '+BASE;},
    f => {f.membership('pending');},
    f => {f.membership(403);},
    f => {f.membership(500);},
    f => {delete f.env.PR_CI_MEMBERS_TOKEN;},
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

test('active org members can approve without repository write access or public membership', async () => {
  const f=fixture();
  f.comment.author_association='NONE'; // A private member need not expose membership on the comment.
  await f.call();
  assert.equal(f.dispatches.length,1);
  assert.deepEqual(f.membershipRequests,[{
    org:'acme', username:'maintainer',
    headers:{authorization:'Bearer test-members-read-token'},
  }]);
  // There is deliberately no repository permission API in this fixture.
  assert.equal(f.github.rest.repos,undefined);
});

test('outside collaborators are rejected even if their comment carries a collaborator badge', async () => {
  const f=fixture(); f.comment.author_association='COLLABORATOR'; f.membership(404);
  await f.call(); assert.equal(f.dispatches.length,0);
});

test('membership is rechecked at admission and completion; suite jobs do not receive its token', async () => {
  const f=fixture(); await f.call();
  f.membership(404);
  await assert.rejects(f.call('admit'),/Membership lookup failed/);
  f.membership('active'); await f.call('admit');
  const lookups=f.membershipRequests.length;
  delete f.env.PR_CI_MEMBERS_TOKEN;
  await f.call('verify');
  assert.equal(f.membershipRequests.length,lookups);
  f.env.PR_CI_MEMBERS_TOKEN='test-members-read-token';
  f.membership(404); f.finish(); await f.call();
  assert.equal(f.checks[0].conclusion,'failure');
});

test('suites are a snapshot and optional labels never dispatch by themselves', async () => {
  const f=fixture(); const optional=config.suites.find(s=>s.label);
  f.pr.labels=[{name:optional.label}];
  f.context.payload.issue.labels=structuredClone(f.pr.labels);
  f.context.eventName='pull_request_target';
  f.context.payload.action='labeled'; f.context.payload.label={name:optional.label};
  await f.call(); assert.equal(f.dispatches.length,0);
  f.context.eventName='issue_comment'; f.context.payload.action='created';
  await f.call(); await f.call('admit');
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
  assert.equal(f.checks[0].status,'queued');
  f.finish(); await f.call();
  assert.equal(f.checks[0].status,'queued');
  assert.equal(JSON.parse(f.checks[0].output.text).comment_id,18);
});

test('unauthorized comments do not cancel legitimate runs', async () => {
  const f=fixture(); await f.call(); await f.call('admit');
  f.comment.id=18; f.membership(404); await f.call();
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
    f=>{f.comment.body='changed';}, f=>{f.membership(404);},
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
  } else {
    assert.deepEqual(selectSuites(['infra/src/test.go'],f.pr,config),['policy','infra']);
    assert.deepEqual(selectSuites(['ts/apps/dashboard/test.ts'],f.pr,config),['policy','vitest','playwright']);
    assert.ok(selectSuites(['ts/apps/www-antfly/app/page.tsx'],f.pr,config).includes('www'));
    assert.deepEqual(selectSuites(['README.md'],f.pr,config),['policy']);
  }
});

test('completion routing uses the orchestrator title, not a branch or arbitrary workflow', () => {
  const f=fixture(); f.finish(); assert.equal(route(f.context),'7');
  f.context.payload.workflow_run.name='Release'; assert.equal(route(f.context),'');
});

test('every expensive worker is gated, pins its checkout, and disables automatic PR triggers', () => {
  const root=path.resolve(__dirname,'../workflows');
  for (const suite of config.suites) {
    const text=fs.readFileSync(path.join(root,suite.workflow),'utf8');
    assert.doesNotMatch(text,/PR_CI_MEMBERS_TOKEN/,suite.workflow);
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
      assert.match(checkout,/ref: \$\{\{ inputs.head_sha \|\| github.sha \}\}/);
      assert.match(checkout,/persist-credentials: false/);
    }
  }
  const admission=fs.readFileSync(path.join(root,'pr-ci-admission.yml'),'utf8');
  assert.doesNotMatch(admission,/PR_CI_MEMBERS_TOKEN/);
  const orchestrator=fs.readFileSync(path.join(root,'pr-ci.yml'),'utf8');
  assert.doesNotMatch(orchestrator,/secrets: inherit/);
  for (const file of fs.readdirSync(root)) {
    const text=fs.readFileSync(path.join(root,file),'utf8');
    if (file!=='pr-ci-controller.yml') assert.doesNotMatch(text,/^  pull_request(?:_target)?:/m,file);
  }
});
