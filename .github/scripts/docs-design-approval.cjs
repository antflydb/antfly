'use strict';

// Trusted metadata-only policy. Never load this file or execute code from a PR.
const MARKER = '<!-- docs-design-exception:v3 -->';
const REPOSITORY = 'antflydb/antfly';

function allowedPath(path) {
  if (typeof path !== 'string' || path.split('/').some(p => !p || p === '.' || p === '..')) return false;
  if (/(^|\/)(AGENTS|CLAUDE|GEMINI)\.md$/i.test(path)) return false;
  if (/(^|\/)(_headers|_redirects|\.assetsignore)$/.test(path)) return false;
  if (['README.md', 'CHANGELOG.md'].includes(path)) return true;
  if (path.startsWith('docs/') && !path.startsWith('docs/security/')) return /\.mdx?$/.test(path);
  return path.startsWith('ts/packages/design-system/src/');
}

function eligible(pr) {
  return pr.state === 'open' && !pr.draft && typeof pr.user?.login === 'string' && pr.user.login.length > 0 &&
    pr.user.type === 'User' && pr.base.ref === 'main' &&
    pr.base.repo?.full_name === REPOSITORY && pr.head.repo?.full_name === REPOSITORY;
}

function validateFiles(pr, files, trees) {
  if (!Number.isInteger(pr.changed_files) || pr.changed_files < 1 ||
      pr.changed_files >= 300 || files.length !== pr.changed_files) return false;
  if (new Set(files.map(f => f.filename)).size !== files.length) return false;
  if (trees.some(tree => tree.truncated || !Array.isArray(tree.tree))) return false;
  const [base, head] = trees.map(tree => new Map(tree.tree.map(item => [item.path, item])));
  const regular = item => item?.type === 'blob' && item.mode === '100644';
  return files.every(file => {
    if (!['added', 'modified', 'removed', 'renamed'].includes(file.status) || !allowedPath(file.filename)) return false;
    if (file.status === 'renamed' && (!file.previous_filename || !allowedPath(file.previous_filename))) return false;
    if (file.status !== 'added' && !regular(base.get(file.previous_filename || file.filename))) return false;
    return file.status === 'removed' || regular(head.get(file.filename));
  });
}

async function main({github, context, core, reviewerLogin}) {
  if (typeof reviewerLogin !== 'string' || !/^[a-z0-9-]+\[bot\]$/.test(reviewerLogin) ||
      reviewerLogin === 'github-actions[bot]') throw new Error('A dedicated GitHub App reviewer is required.');
  if (`${context.repo.owner}/${context.repo.repo}` !== REPOSITORY ||
      context.eventName !== 'pull_request_target') throw new Error('Unexpected repository or event.');
  const args = {...context.repo, pull_number: context.payload.pull_request.number};
  const getPR = async () => (await github.rest.pulls.get(args)).data;
  const pr = await getPR();
  if (!eligible(pr)) return core.notice('Human review required: PR is outside the docs/design exception.');
  const {data: access} = await github.rest.repos.getCollaboratorPermissionLevel({
    ...context.repo, username: pr.user.login,
  });
  if (!['write', 'maintain', 'admin'].includes(access.permission)) throw new Error('PR author does not have repository write access.');
  // PR file listings follow a mutable branch. Compare immutable SHAs instead,
  // so an A -> B -> A force-push cannot substitute B's allowed paths for A's.
  // GitHub returns at most 300 files on page one; the boundary fails closed.
  const {data: comparison} = await github.rest.repos.compareCommitsWithBasehead({
    ...context.repo, basehead: `${pr.base.sha}...${pr.head.sha}`, per_page: 1, page: 1,
  });
  if (comparison.base_commit?.sha !== pr.base.sha ||
      !/^[a-f0-9]{40}$/.test(comparison.merge_base_commit?.sha || '') ||
      !Array.isArray(comparison.files) || comparison.files.length >= 300 || comparison.truncated) {
    throw new Error('Incomplete or mismatched immutable commit comparison; human review required.');
  }
  const files = comparison.files;
  // Deleted/renamed paths belong to the merge base, not a newer base-branch tip.
  const trees = await Promise.all([comparison.merge_base_commit.sha, pr.head.sha].map(async tree_sha =>
    (await github.rest.git.getTree({...context.repo, tree_sha, recursive: '1'})).data));
  if (!validateFiles(pr, files, trees)) return core.notice('Human review required: changed files are outside the exception.');
  // Fetch again after comparison/tree reads. Never approve a newer, uninspected head.
  const current = await getPR();
  if (!eligible(current) || current.head.sha !== pr.head.sha || current.base.sha !== pr.base.sha ||
      current.changed_files !== pr.changed_files || current.user.login !== pr.user.login) throw new Error('PR changed during inspection; retry on the latest event.');
  const reviews = await github.paginate(github.rest.pulls.listReviews, {...args, per_page: 100});
  if (reviews.some(review => review.user?.login === reviewerLogin &&
      review.state === 'APPROVED' && review.commit_id === pr.head.sha && review.body?.includes(MARKER))) {
    return core.notice('This exact head already has the scoped policy approval.');
  }
  await github.rest.pulls.createReview({...args, commit_id: pr.head.sha, event: 'APPROVE',
    body: `${MARKER}\nAutomatic policy approval for docs/design-only change by @${pr.user.login} at ${pr.head.sha}. ` +
      'This is an authorized review exception, not a human code review. Required CI and conversation resolution still apply.'});
}

module.exports = {allowedPath, eligible, validateFiles, main};
