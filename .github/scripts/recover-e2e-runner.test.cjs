const {test} = require('node:test');
const assert = require('node:assert/strict');
const {interruptedE2e, recover} = require('./recover-e2e-runner.cjs');
const shutdown = '##[error]The runner has received a shutdown signal.';
const job = () => ({id: 7, name: 'e2e-full / inference-speech', conclusion: 'failure', steps: [
  {name: 'Set up job', conclusion: 'success'},
  {name: 'Run Antfly inference full E2E', conclusion: 'failure'},
  {name: 'Upload logs', conclusion: 'skipped'},
]});

test('only interrupted runtime lanes qualify', () => {
  assert.equal(interruptedE2e(job(), shutdown), true);
  const preempted = job(); preempted.steps[1].conclusion = 'cancelled';
  assert.equal(interruptedE2e(preempted, shutdown), true);
  assert.equal(interruptedE2e(job(), 'network timeout'), false);
  assert.equal(interruptedE2e({...job(), name: 'zig-full / x86_64'}, shutdown), false);
  assert.equal(interruptedE2e({...job(), conclusion: 'cancelled'}, shutdown), false);
  const setup = job(); setup.steps[0].conclusion = 'failure';
  assert.equal(interruptedE2e(setup, shutdown), false);
});

test('preexisting test failures and test deadlines remain failures', () => {
  for (const output of [
    'test_dictate.py::test_cleanup FAILED [ 24%]',
    'test_dictate.py::test_cleanup ERROR [100%]',
    'FAILED e2e/inference/test_dictate.py::test_cleanup',
    'ERROR zig/e2e/antfly/test_graph.py::test_graph',
    'The job running on runner has exceeded the maximum execution time',
  ]) assert.equal(interruptedE2e(job(), `${output}\n${shutdown}`), false, output);
});

async function attempt({override = {}, log = shutdown, currentHead = 'abc', secondRun, secondHead, jobs = [job()]} = {}) {
  const run = {id: 1, run_attempt: 1, event: 'push', path: '.github/workflows/zig-tests.yml',
    head_branch: 'main', head_sha: 'abc', head_repository: {full_name: 'antflydb/antfly'},
    status: 'completed', conclusion: 'failure', ...override};
  const posts = [];
  let reads = 0, headReads = 0;
  await recover({context: {repo: {owner: 'antflydb', repo: 'antfly'}, payload: {workflow_run: run}},
    core: {info() {}}, github: {rest: {actions: {
      getWorkflowRun: async () => ({data: ++reads > 1 && secondRun ? {...run, ...secondRun} : run}),
      listJobsForWorkflowRun: 'jobs',
    }, git: {getRef: async () => ({data: {object: {sha: ++headReads > 1 && secondHead ? secondHead : currentHead}}})}},
    paginate: async () => jobs,
    request: async (route, args) => {if (route.startsWith('POST')) posts.push(args); return {data: log};}}});
  return posts;
}

test('only the interrupted job is rerun, and at most one retry is scheduled', async () => {
  assert.deepEqual(await attempt({jobs: [job(), {...job(), id: 8}]}), [{owner: 'antflydb', repo: 'antfly', job_id: 7}]);
  assert.deepEqual(await attempt({override: {event: 'schedule'}}), [{owner: 'antflydb', repo: 'antfly', job_id: 7}]);
  for (const override of [{run_attempt: 2}, {status: 'in_progress'}, {conclusion: 'cancelled'},
    {head_branch: 'feature'}, {event: 'workflow_dispatch'}, {event: 'pull_request'},
    {path: '.github/workflows/other.yml'}, {head_repository: {full_name: 'someone/antfly'}}]) {
    assert.deepEqual(await attempt({override}), [], JSON.stringify(override));
  }
});

test('superseded heads and racing reruns cannot consume another runner', async () => {
  assert.deepEqual(await attempt({currentHead: 'new'}), []);
  assert.deepEqual(await attempt({secondHead: 'new'}), []);
  assert.deepEqual(await attempt({secondRun: {run_attempt: 2}}), []);
  assert.deepEqual(await attempt({secondRun: {status: 'in_progress'}}), []);
  assert.deepEqual(await attempt({secondRun: {conclusion: 'cancelled'}}), []);
  assert.deepEqual(await attempt({log: 'assertion failed without runner shutdown'}), []);
});
