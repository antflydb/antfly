// Trusted control-plane code. Never load this file from a PR checkout.
// Kept identical in antfly and colony; repository-specific suites live in JSON.
'use strict';

const CHECK = 'PR CI';
const WORKFLOW = 'pr-ci.yml';
const COMMAND = /^\/ci run ([a-f0-9]{40})\s*$/;
const TITLE = /^PR CI #(\d+) \/ check (\d+) \/ approval (\d+)$/;

function route(context) {
  const p = context.payload;
  if (context.eventName === 'workflow_run') {
    return p.workflow_run.path === `.github/workflows/${WORKFLOW}`
      ? (p.workflow_run.display_title.match(TITLE)?.[1] || '') : '';
  }
  return String(p.pull_request?.number || (p.issue?.pull_request && p.issue.number) || '');
}

function labels(pr, config) {
  const known = new Set(config.suites.map(s => s.label).filter(Boolean));
  return pr.labels.map(l => l.name).filter(l => known.has(l)).sort();
}

function selectSuites(files, pr, config) {
  const selectedLabels = labels(pr, config);
  return config.suites.filter(s => s.label
    ? selectedLabels.includes(s.label)
    : !s.paths || files.some(f => s.paths.some(p => new RegExp(p).test(f))))
    .map(s => s.id);
}

function validatePR(pr, repo) {
  if (pr.state !== 'open' || pr.draft) throw new Error('PR must be open and non-draft.');
  if (pr.head.repo?.full_name !== repo || pr.base.repo.full_name !== repo) {
    throw new Error('Self-hosted PR CI supports same-repository PRs only.');
  }
}

function validateSnapshot(pr, approval, config) {
  validatePR(pr, approval.repository);
  if (pr.head.sha !== approval.sha || pr.base.sha !== approval.base_sha ||
      pr.base.ref !== approval.base_ref ||
      JSON.stringify(labels(pr, config)) !== JSON.stringify(approval.labels)) {
    throw new Error('Commit, base, or selected suites changed; post a fresh approval.');
  }
}

function metadata(check) {
  if (check.name !== CHECK || check.app?.slug !== 'github-actions' ||
      !check.external_id?.startsWith('pr-ci:')) throw new Error('Not a trusted PR CI check.');
  return JSON.parse(check.output.text);
}

async function main({github, context, core, mode, config, env = process.env}) {
  const repo = context.repo;
  const repository = `${repo.owner}/${repo.repo}`;
  const number = Number(env.PR_NUMBER || route(context));
  const getPR = async () => (await github.rest.pulls.get({...repo, pull_number: number})).data;
  const getCheck = async id => (await github.rest.checks.get({...repo, check_run_id: Number(id)})).data;
  const writeCheck = async (check, data, status, summary, conclusion) => {
    const body = {
      ...repo, check_run_id: check.id, status,
      output: {title: CHECK, summary, text: JSON.stringify(data)},
    };
    if (conclusion) body.conclusion = conclusion;
    await github.rest.checks.update(body);
  };
  const checkFor = async pr => {
    const checks = await github.paginate(github.rest.checks.listForRef, {
      ...repo, ref: pr.head.sha, check_name: CHECK, filter: 'all', per_page: 100,
    });
    return checks.find(c => c.external_id?.startsWith(`pr-ci:${number}:`) &&
      c.app?.slug === 'github-actions');
  };
  const save = async (pr, data, summary, approved = false) => {
    let check = await checkFor(pr);
    // Retain the last consumed comment ID across invalidation. A redelivered
    // old webhook must not purchase another run after draft/label changes.
    if (check && !approved) data = {...metadata(check), ...data};
    // A completed GitHub check cannot be requeued by changing status alone.
    // Each approval gets a fresh check; the newest record retains replay protection.
    if (!check || approved) {
      check = (await github.rest.checks.create({
        ...repo, name: CHECK, head_sha: pr.head.sha,
        external_id: `pr-ci:${number}:v1`, status: 'queued',
        output: {title: CHECK, summary, text: JSON.stringify(data)},
      })).data;
    }
    await writeCheck(check, data, approved ? 'queued' : 'completed', summary,
      approved ? undefined : 'action_required');
    return check;
  };
  const human = async comment => {
    if (comment.user.type !== 'User' || comment.user.login.endsWith('[bot]')) {
      throw new Error('Approval must come from a human maintainer account.');
    }
    const {data} = await github.rest.repos.getCollaboratorPermissionLevel({
      ...repo, username: comment.user.login,
    });
    if (!['write', 'maintain', 'admin'].includes(data.permission) && !data.user?.permissions?.push) {
      throw new Error('Approval requires repository write access.');
    }
  };
  const validateComment = async approval => {
    const {data: comment} = await github.rest.issues.getComment({...repo, comment_id: approval.comment_id});
    if (comment.issue_url !== `${context.apiUrl}/repos/${repository}/issues/${number}` ||
        comment.body.trim() !== `/ci run ${approval.sha}` ||
        comment.updated_at !== comment.created_at || comment.user.login !== approval.approver) {
      throw new Error('Approval comment was edited, moved, or no longer matches.');
    }
    await human(comment);
  };
  const cancel = async () => {
    // Only cancel this PR's orchestrator, never main, schedules, or releases.
    for (const status of ['queued', 'in_progress', 'waiting', 'pending', 'requested']) {
      const runs = await github.paginate(github.rest.actions.listWorkflowRuns, {
        ...repo, workflow_id: WORKFLOW, status, per_page: 100,
      });
      for (const run of runs.filter(r => r.display_title?.match(TITLE)?.[1] === String(number))) {
        const id = run.display_title.match(TITLE)[2];
        const check = await getCheck(id);
        const data = metadata(check);
        if (data.comment_id === Number(run.display_title.match(TITLE)[3])) {
          await writeCheck(check, {...data, revoked: true}, 'completed',
            'Approval invalidated. Post a fresh /ci run <full-head-sha>.', 'action_required');
        }
        try {
          await github.rest.actions.cancelWorkflowRun({...repo, run_id: run.id});
        } catch (error) {
          if (error.status !== 409) throw error; // Already finished.
        }
      }
    }
  };

  if (mode === 'admit' || mode === 'verify') {
    if (context.runAttempt !== undefined && Number(context.runAttempt) !== 1 ||
        Number(env.GITHUB_RUN_ATTEMPT || 1) !== 1) {
      throw new Error('Reruns require a new approval comment.');
    }
    if (context.ref !== `refs/heads/${context.payload.repository.default_branch}`) {
      throw new Error('PR CI must use the default-branch workflow definition.');
    }
    const check = await getCheck(env.CHECK_ID);
    const data = metadata(check);
    if (data.repository !== repository || data.number !== number || data.revoked ||
        data.comment_id !== Number(env.COMMENT_ID) || check.head_sha !== data.sha ||
        !data.suites?.length) throw new Error('Invalid approval record.');
    if (mode === 'admit' ? check.status !== 'queued' || data.run_id
      : check.status !== 'in_progress' || data.run_id !== context.runId) {
      throw new Error('Approval is expired, already used, or belongs to another run.');
    }
    if (mode === 'verify' && (env.HEAD_SHA !== data.sha || env.BASE_SHA !== data.base_sha)) {
      throw new Error('Suite checkout does not match the approved commit and base.');
    }
    validateSnapshot(await getPR(), data, config);
    await validateComment(data);
    if (mode === 'admit') {
      data.run_id = context.runId;
      await writeCheck(check, data, 'in_progress',
        `Approved by @${data.approver}: ${data.suites.join(', ')} at ${data.sha}.`);
    }
    if (env.SUITE && !data.suites.includes(env.SUITE)) throw new Error('Suite was not approved.');
    for (const [key, value] of Object.entries({
      head_sha: data.sha, base_sha: data.base_sha, pr_number: data.number,
      check_id: check.id, comment_id: data.comment_id, suites: JSON.stringify(data.suites),
    })) core.setOutput(key, String(value));
    return;
  }

  if (mode !== 'event' || !number) throw new Error('Invalid controller mode or PR number.');
  if (context.eventName === 'workflow_run') {
    const run = context.payload.workflow_run;
    const match = run.display_title.match(TITLE);
    const check = await getCheck(match[2]);
    const data = metadata(check);
    if ((data.run_id && data.run_id !== run.id) || data.comment_id !== Number(match[3]) || data.revoked) return;
    try {
      if (run.path !== `.github/workflows/${WORKFLOW}` || run.event !== 'workflow_dispatch' ||
          run.head_branch !== context.payload.repository.default_branch || run.run_attempt !== 1) {
        throw new Error('Unexpected workflow definition, event, or rerun.');
      }
      validateSnapshot(await getPR(), data, config);
      await validateComment(data);
      // A skipped/failed admission or suite must never become a passing check.
      const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {
        ...repo, run_id: run.id, filter: 'latest', per_page: 100,
      });
      const result = jobs.find(j => j.name === 'PR CI result');
      if (!data.run_id || run.conclusion !== 'success' || result?.conclusion !== 'success') {
        throw new Error(`CI did not pass (${run.conclusion}). Post a new approval to retry.`);
      }
      await writeCheck(check, data, 'completed',
        `Passed ${data.suites.join(', ')} at ${data.sha}; approved by @${data.approver}.`, 'success');
    } catch (error) {
      await writeCheck(check, {...data, revoked: true}, 'completed', error.message, 'failure');
    }
    return;
  }

  const pr = await getPR(); // Always use live state, not a stale webhook snapshot.
  const p = context.payload;
  if (context.eventName === 'issue_comment') {
    if (p.action !== 'created') {
      const check = await checkFor(pr);
      if (!check || metadata(check).comment_id !== p.comment.id) return;
      await cancel();
      await save(pr, {number, revoked: true}, 'Approval comment changed. Post a new approval.');
      return;
    }
    const match = p.comment.body.trim().match(COMMAND);
    if (!match) return;
    try {
      await human(p.comment);
      validatePR(pr, repository);
      if (match[1] !== pr.head.sha || p.comment.created_at !== p.comment.updated_at) {
        throw new Error('Use an unedited /ci run comment with the current full head SHA.');
      }
      if (JSON.stringify(labels(p.issue, config)) !== JSON.stringify(labels(pr, config))) {
        throw new Error('Suite labels changed after the approval comment; post a fresh approval.');
      }
    } catch (error) {
      // An unauthorized comment must not cancel a legitimate approved run.
      core.notice(error.message);
      return;
    }
    const files = await github.paginate(github.rest.pulls.listFiles, {...repo, pull_number: number, per_page: 100});
    // GitHub caps this API at 3,000 files. Fall back to all normal suites.
    const suites = files.length >= 3000
      ? config.suites.filter(s => !s.label || labels(pr, config).includes(s.label)).map(s => s.id)
      : selectSuites(files.flatMap(f => [f.filename, f.previous_filename].filter(Boolean)), pr, config);
    const approval = {
      repository, number, sha: pr.head.sha, base_sha: pr.base.sha, base_ref: pr.base.ref,
      comment_id: p.comment.id, approver: p.comment.user.login,
      labels: labels(pr, config), suites,
    };
    // Comments may be delivered twice; never reuse an already consumed approval.
    const old = await checkFor(pr);
    if (old && metadata(old).comment_id >= approval.comment_id) return;
    await cancel();
    const check = await save(pr, approval, `Approved by @${approval.approver}; waiting to start.`, true);
    try {
      await github.rest.actions.createWorkflowDispatch({
        ...repo, workflow_id: WORKFLOW, ref: p.repository.default_branch,
        inputs: {pr_number: String(number), check_id: String(check.id), comment_id: String(p.comment.id)},
      });
    } catch (error) {
      await writeCheck(check, {...approval, revoked: true}, 'completed', 'Dispatch failed; post a fresh approval.', 'failure');
      throw error;
    }
    return;
  }

  if (['labeled', 'unlabeled'].includes(p.action) &&
      !config.suites.some(s => s.label === p.label?.name)) return;
  if (p.action === 'edited' && !p.changes?.base) return;
  await cancel();
  await save(pr, {number, revoked: true}, pr.draft
    ? 'Draft PR: test runners remain idle.'
    : `Awaiting human approval. Select suite labels, then comment /ci run ${pr.head.sha}.`);
}

module.exports = {main, route, labels, selectSuites, validatePR, validateSnapshot, metadata};
