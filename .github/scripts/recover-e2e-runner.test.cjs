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

function buildJob(step = 'Build E2E executable once') {
  return {...job(), name: 'e2e-full / build', steps: [
    {name: 'Set up job', number: 1, conclusion: 'success'},
    {name: step, number: 10, conclusion: 'cancelled'},
  ]};
}

test('externally interrupted E2E compilers qualify, but build failures remain failed', () => {
  for (const step of ['Build E2E executable once', 'Build optimized model E2E executable']) {
    assert.equal(interruptedE2e(buildJob(step), `KeyboardInterrupt\n${shutdown}`), true);
    for (const failure of ['error: OutOfMemory', 'error: process terminated with signal ABRT',
      'failed command: ./capi-smoke', 'FAIL (TestExpectedEqual)',
      'The job running on runner has exceeded the maximum execution time']) {
      assert.equal(interruptedE2e(buildJob(step), `${failure}\n${shutdown}`), false, failure);
    }
  }
  assert.equal(interruptedE2e(buildJob(), 'compiler exited without shutdown'), false);
  assert.equal(interruptedE2e(buildJob('Upload E2E executable'), shutdown), false);
  const failed = buildJob(); failed.steps[1].conclusion = 'failure';
  assert.equal(interruptedE2e(failed, shutdown), false);
});

function interruptedShellJob() {
  const interrupted = buildJob('Build optimized model E2E executable');
  interrupted.steps[1].conclusion = 'failure';
  interrupted.steps.push({name: 'Release completed E2E build scratch', number: 16, conclusion: 'cancelled'});
  return interrupted;
}

const shellShutdown = code => `KeyboardInterrupt\n##[error]Process completed with exit code ${code}.\n${shutdown}`;

test('interrupted build shells can have cancelled teardown, but independent failures cannot', () => {
  for (const code of [130, 143]) {
    assert.equal(interruptedE2e(interruptedShellJob(), shellShutdown(code)), true);
    const post = interruptedShellJob();
    post.steps.push({name: 'Post Sample ARC runner disk use', number: 31, conclusion: 'cancelled'});
    assert.equal(interruptedE2e(post, shellShutdown(code)), true);
    for (const [name, number, conclusion] of [
      ['Install build tools', 5, 'failure'],
      ['Post Check out executing dependency policy', 29, 'failure'],
      ['Release completed E2E build scratch', 16, 'failure'],
      ['Upload E2E executable', 13, 'cancelled'],
      ['Post earlier action', 9, 'cancelled'],
      ['Build E2E executable once', 9, 'failure'],
    ]) {
      const independent = interruptedShellJob();
      independent.steps.push({name, number, conclusion});
      assert.equal(interruptedE2e(independent, shellShutdown(code)), false, name);
    }
    for (const failure of ['error: OutOfMemory', 'FAIL (TestExpectedEqual)', 'failed command: ./capi-smoke']) {
      assert.equal(interruptedE2e(interruptedShellJob(), `${failure}\n${shellShutdown(code)}`), false);
    }
  }
  for (const code of [1, 2, 137]) assert.equal(interruptedE2e(interruptedShellJob(), shellShutdown(code)), false);
  const cancelled = buildJob('Build optimized model E2E executable');
  for (const code of [1, 2, 137]) assert.equal(interruptedE2e(cancelled, shellShutdown(code)), false);
  assert.equal(interruptedE2e(interruptedShellJob(), `${shellShutdown(137)}\n${shellShutdown(130)}`), false);
  assert.equal(interruptedE2e(interruptedShellJob(), shutdown), false);
  assert.equal(interruptedE2e({...interruptedShellJob(), conclusion: 'cancelled'}, shellShutdown(130)), false);
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
  assert.deepEqual(await attempt({jobs: [buildJob()]}), [{owner: 'antflydb', repo: 'antfly', job_id: 7}]);
  assert.deepEqual(await attempt({jobs: [buildJob()], override: {run_attempt: 2}}), []);
  assert.deepEqual(await attempt({jobs: [interruptedShellJob()], log: shellShutdown(130)}), [{owner: 'antflydb', repo: 'antfly', job_id: 7}]);
  assert.deepEqual(await attempt({jobs: [interruptedShellJob()], log: shellShutdown(130), override: {run_attempt: 2}}), []);
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
