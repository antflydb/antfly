const {test} = require('node:test');
const assert = require('node:assert/strict');
const {interruptedCheckout, recover} = require('./recover-vopr-checkout.cjs');
const log = '##[error]The runner has received a shutdown signal.';
const job = () => ({id: 7, name: 'qualify', conclusion: 'failure', steps: [
  {number: 1, name: 'Set up job', conclusion: 'success'},
  {number: 2, name: 'Run actions/checkout@pinned', conclusion: 'failure'},
  {number: 3, name: 'Compile and test', conclusion: 'skipped'},
  {number: 4, name: 'Post Run actions/checkout@pinned', conclusion: 'cancelled'},
  {number: 5, name: 'Complete job', conclusion: 'success'},
]});
test('only checkout shutdown qualifies; test failures and network errors do not', () => {
  assert.equal(interruptedCheckout(job(), log), true);
  assert.equal(interruptedCheckout(job(), 'network failed'), false);
  for (const conclusion of ['success', 'failure', 'cancelled']) {
    const candidate = job(); candidate.steps[2].conclusion = conclusion;
    assert.equal(interruptedCheckout(candidate, log), false);
  }
});
async function attempt(overrides = {}, jobs = [job()]) {
  const run = {id: 1, run_attempt: 1, event: 'schedule', path: '.github/workflows/zig-vopr-soak.yml',
    status: 'completed', conclusion: 'failure', head_repository: {full_name: 'antflydb/antfly'}, ...overrides};
  const requests = [];
  await recover({context: {repo: {owner: 'antflydb', repo: 'antfly'}, payload: {workflow_run: run}},
    core: {info() {}}, github: {rest: {actions: {
      getWorkflowRun: async () => ({data: run}), listJobsForWorkflowRun: 'jobs',
    }}, paginate: async () => jobs, request: async (route, args) => {
      requests.push({route, args}); return {data: log};
    }}});
  return requests.filter(request => request.route.startsWith('POST'));
}
test('recovery reruns the checkout job only, once', async () => {
  const requests = await attempt();
  assert.equal(requests.length, 1);
  assert.equal(requests[0].args.job_id, 7);
  assert.deepEqual(await attempt({run_attempt: 2}), []);
  assert.deepEqual(await attempt({status: 'in_progress'}), []);
});
test('PRs, foreign repositories and executed downstream jobs cannot be rerun', async () => {
  assert.deepEqual(await attempt({event: 'pull_request'}), []);
  assert.deepEqual(await attempt({head_repository: {full_name: 'someone/antfly'}}), []);
  assert.deepEqual(await attempt({}, [job(), {name: 'campaign (raft, 0)', conclusion: 'success'}]), []);
});
