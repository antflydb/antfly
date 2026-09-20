// Recover only an interrupted checkout: no compiler or test execution is retried.
const SHUTDOWN = 'The runner has received a shutdown signal.';

function interruptedCheckout(job, log) {
  if (job.name !== 'qualify' || job.conclusion !== 'failure') return false;
  const steps = job.steps || [];
  const checkout = steps.find(step => /^Run actions\/checkout@/.test(step.name));
  if (!checkout || checkout.conclusion !== 'failure') return false;
  const work = steps.filter(step => !step.name.startsWith('Post ') && step.name !== 'Complete job');
  return work.every(step => step.number < checkout.number
    ? step.name === 'Set up job' && step.conclusion === 'success'
    : step.number === checkout.number || step.conclusion === 'skipped') && log.includes(SHUTDOWN);
}

async function recover({github, context, core}) {
  const event = context.payload.workflow_run;
  if (!event || event.event !== 'schedule' || event.path !== '.github/workflows/zig-vopr-soak.yml') return;
  const {owner, repo} = context.repo;
  const {data: run} = await github.rest.actions.getWorkflowRun({owner, repo, run_id: event.id});
  // Re-read authoritative state: duplicate/delayed completion events must not
  // queue a second retry, or act on a running attempt.
  if (run.run_attempt !== 1 || run.status !== 'completed' || run.conclusion !== 'failure' ||
      run.event !== 'schedule' || run.head_repository?.full_name !== `${owner}/${repo}` ||
      run.path !== '.github/workflows/zig-vopr-soak.yml') return;
  const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {
    owner, repo, run_id: run.id, filter: 'latest', per_page: 100,
  });
  const job = jobs.find(job => job.name === 'qualify' && job.conclusion === 'failure');
  if (!job) return;
  // The API also reruns dependent jobs. They must never have executed.
  if (jobs.some(job => /^(campaign|corpus)(\s|$)/.test(job.name) && job.conclusion !== 'skipped')) return;
  const response = await github.request('GET /repos/{owner}/{repo}/actions/jobs/{job_id}/logs', {
    owner, repo, job_id: job.id,
  });
  const log = typeof response.data === 'string' ? response.data : Buffer.from(response.data).toString('utf8');
  if (!interruptedCheckout(job, log)) return;
  await github.request('POST /repos/{owner}/{repo}/actions/jobs/{job_id}/rerun', {
    owner, repo, job_id: job.id,
  });
  core.info(`Recovered pre-test runner shutdown for job ${job.id}; one retry only.`);
}

module.exports = {interruptedCheckout, recover};
