// Retry externally interrupted main E2E jobs once. Test failures stay failed.
const WORKFLOW = '.github/workflows/zig-tests.yml';
const SHUTDOWN = 'The runner has received a shutdown signal.';

function interruptedE2e(job, log) {
  if (job.conclusion !== 'failure') return false;
  const build = job.name === 'e2e-full / build';
  if (!build && !/^e2e-full \/ (inference-(generation|speech|embedding)|antfly)$/.test(job.name)) return false;
  // Runner loss marks the executing step cancelled even when the enclosing
  // job fails. A deliberately cancelled job is excluded above.
  const failed = (job.steps || []).filter(step => ['failure', 'cancelled'].includes(step.conclusion));
  if (failed.length !== 1) return false;
  if (build) {
    if (failed[0].conclusion !== 'cancelled' ||
        !/^Build (E2E executable once|optimized model E2E executable)$/.test(failed[0].name)) return false;
  } else if (!/^Run Antfly (inference )?full E2E$/.test(failed[0].name)) return false;
  const shutdown = log.indexOf(SHUTDOWN);
  if (shutdown < 0 || /exceeded the maximum execution time/.test(log)) return false;
  const before = log.slice(0, shutdown);
  // Rebuild only externally interrupted compilers. A compiler/smoke failure
  // already present before shutdown must remain visible. The job-specific
  // rerun also schedules its dependent artifact consumers, preserving the
  // completed unit and SDK work instead of repeating the entire workflow.
  if (build) return !/(?:^|\n)[^\n]*\berror:|\bFAIL(?:ED)?\b|failed command:/.test(before);
  // These lanes use verbose pytest progress. Do not erase an assertion failure
  // that happened before the runner was lost or retry explicit test deadlines.
  return !/\b(?:FAILED|ERROR)\s+\[\s*\d+%\]|\b(?:FAILED|ERROR)\s+(?:zig\/)?e2e\//.test(before);
}

async function recover({github, context, core}) {
  const event = context.payload.workflow_run;
  if (!event || event.path !== WORKFLOW || !['push', 'schedule'].includes(event.event)) return;
  const {owner, repo} = context.repo;
  const {data: run} = await github.rest.actions.getWorkflowRun({owner, repo, run_id: event.id});
  if (run.path !== WORKFLOW || run.head_repository?.full_name !== `${owner}/${repo}` ||
      run.head_branch !== 'main' || !['push', 'schedule'].includes(run.event) ||
      run.run_attempt !== 1 || run.status !== 'completed' || run.conclusion !== 'failure') return;
  const {data: head} = await github.rest.git.getRef({owner, repo, ref: 'heads/main'});
  if (head.object.sha !== run.head_sha) return;
  const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {
    owner, repo, run_id: run.id, filter: 'latest', per_page: 100,
  });
  for (const job of jobs) {
    if (job.conclusion !== 'failure' || !/^e2e-full \/ /.test(job.name)) continue;
    const response = await github.request('GET /repos/{owner}/{repo}/actions/jobs/{job_id}/logs', {
      owner, repo, job_id: job.id,
    });
    const log = typeof response.data === 'string' ? response.data : Buffer.from(response.data).toString('utf8');
    if (!interruptedE2e(job, log)) continue;
    // Recheck just before scheduling: superseded main commits must not consume
    // runners, and a duplicate completion must not create a third attempt.
    const {data: current} = await github.rest.actions.getWorkflowRun({owner, repo, run_id: run.id});
    const {data: latest} = await github.rest.git.getRef({owner, repo, ref: 'heads/main'});
    if (current.run_attempt !== 1 || current.status !== 'completed' || current.conclusion !== 'failure' ||
        latest.object.sha !== run.head_sha) return;
    await github.request('POST /repos/{owner}/{repo}/actions/jobs/{job_id}/rerun', {owner, repo, job_id: job.id});
    core.info(`Recovered interrupted main E2E job ${job.id}; one retry only.`);
    // The rerun API advances the run attempt. Let one job own that attempt;
    // this also bounds total automatic work when multiple runners were lost.
    return;
  }
}

module.exports = {interruptedE2e, recover};
