#!/usr/bin/env python3
"""Publish only a validated Colony Antfly gitlink using the existing dispatch PAT.

Runs in Antfly, never checks out Colony or executes its code. Tokens remain in
this publishing process; archive redirects are fetched without Authorization.
"""
import io
import json
import os
import re
import time
import urllib.error
import urllib.request
import zipfile

COLONY = 'antflydb/colony'
UPSTREAM = 'antflydb/antfly'
WORKFLOW = 'update-antfly-submodule.yml'
API_ROOT = 'https://api.github.com/'
SHA = re.compile(r'[0-9a-f]{40}')


class PublicationError(ValueError):
    """Only deliberately sanitized, public-safe diagnostics use this type."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class GitHub:
    def __init__(self, token):
        self.token = token
        self.opener = urllib.request.build_opener(NoRedirect)

    def request(self, path, method='GET', body=None):
        request = urllib.request.Request(API_ROOT+path, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization': 'Bearer '+self.token, 'Accept': 'application/vnd.github+json',
                     'Content-Type': 'application/json', 'X-GitHub-Api-Version': '2022-11-28'})
        try:
            with self.opener.open(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            area = 'Actions read' if '/actions/' in path else 'repository contents'
            raise PublicationError(f'GitHub {method} {area} failed with HTTP {exc.code}; no token/body logged') from None

    def archive(self, artifact_id):
        request = urllib.request.Request(API_ROOT+f'repos/{COLONY}/actions/artifacts/{int(artifact_id)}/zip',
            headers={'Authorization': 'Bearer '+self.token, 'Accept': 'application/vnd.github+json'})
        try:
            response = self.opener.open(request, timeout=30)
        except urllib.error.HTTPError as exc:
            if exc.code != 302:
                raise PublicationError(f'Artifact download failed: HTTP {exc.code}') from None
            location = exc.headers.get('Location', '')
            if not location.startswith('https://'):
                raise PublicationError('Artifact redirect must use HTTPS')
            # Deliberately a new unauthenticated request, never forwarding the PAT.
            response = urllib.request.urlopen(location, timeout=30)
        with response:
            data = response.read(65537)
        if len(data) > 65536:
            raise PublicationError('Unexpectedly large validation artifact')
        return data


def require(condition, message):
    if not condition:
        raise PublicationError(message)


def read_manifest(data):
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        require(archive.namelist() == ['pin.json'], 'Unexpected artifact members')
        require(archive.getinfo('pin.json').file_size <= 4096, 'Manifest is too large')
        result = json.loads(archive.read('pin.json'))
    require(set(result) == {'version', 'request', 'base_sha', 'old_sha', 'target_sha', 'changed'}, 'Invalid manifest fields')
    require(result['version'] == 1 and type(result['changed']) is bool, 'Invalid manifest version/change flag')
    require(all(isinstance(result[key], str) and SHA.fullmatch(result[key]) for key in ('base_sha', 'old_sha', 'target_sha')), 'Invalid commit identifiers')
    require(result['changed'] == (result['old_sha'] != result['target_sha']), 'Inconsistent change flag')
    return result


def wait_for_validation(api, request, timeout=2100):
    deadline = time.monotonic()+timeout
    while time.monotonic() < deadline:
        runs = api.request(f'repos/{COLONY}/actions/workflows/{WORKFLOW}/runs?event=repository_dispatch&per_page=100')['workflow_runs']
        matching = [run for run in runs if run.get('display_title') == 'Antfly pin validation '+request]
        if matching:
            run = matching[0]
            require(run['event'] == 'repository_dispatch' and run['head_branch'] == 'main' and
                    run['head_repository']['full_name'] == COLONY, 'Untrusted validation run')
            if run['status'] == 'completed':
                require(run['conclusion'] == 'success', 'Candidate validation did not succeed; main unchanged')
                artifacts = api.request(f"repos/{COLONY}/actions/runs/{run['id']}/artifacts")['artifacts']
                selected = [a for a in artifacts if a['name'] == 'antfly-pin-'+request and not a['expired']]
                require(len(selected) == 1, 'Expected exactly one successful validation manifest')
                manifest = read_manifest(api.archive(selected[0]['id']))
                require(manifest['request'] == request and manifest['base_sha'] == run['head_sha'], 'Manifest is not bound to this validation run/base')
                return run, manifest
        time.sleep(15)
    raise PublicationError('Timed out waiting for private Colony validation; main unchanged')


def entries(tree):
    require(not tree.get('truncated'), 'Truncated root tree')
    return {row['path']: (row['mode'], row['type'], row['sha']) for row in tree['tree']}


def publish(api, run, manifest):
    require(run['conclusion'] == 'success' and manifest['base_sha'] == run['head_sha'], 'Validation is required')
    base, old, target = (manifest[key] for key in ('base_sha', 'old_sha', 'target_sha'))
    current = api.request(f'repos/{COLONY}/git/ref/heads/main')['object']['sha']
    require(current == base, 'Colony main advanced during validation; retry validation against the new base')
    upstream = api.request(f'repos/{UPSTREAM}/compare/{old}...{target}')
    require(upstream['status'] in ('ahead', 'identical'), 'Automatic pin updates must be forward-only')
    reachable = api.request(f'repos/{UPSTREAM}/compare/{target}...main')
    require(reachable['status'] in ('ahead', 'identical'), 'Candidate is no longer on Antfly main')
    parent = api.request(f'repos/{COLONY}/git/commits/{base}')
    original = api.request(f"repos/{COLONY}/git/trees/{parent['tree']['sha']}")
    before = entries(original)
    require(before.get('antfly') == ('160000', 'commit', old), 'Validated old gitlink does not match Colony')
    if not manifest['changed']:
        print('Antfly pin is already current; no commit created')
        return None
    tree = api.request(f'repos/{COLONY}/git/trees', 'POST', {
        'base_tree': parent['tree']['sha'],
        'tree': [{'path': 'antfly', 'mode': '160000', 'type': 'commit', 'sha': target}]})
    expected = dict(before, antfly=('160000', 'commit', target))
    require(entries(tree) == expected, 'Refuse a tree that changes anything except the Antfly gitlink')
    commit = api.request(f'repos/{COLONY}/git/commits', 'POST', {
        'message': f"chore: update antfly submodule\n\nAntfly-Source: {target}\nPrevious-Pin: {old}\nValidated-Run: https://github.com/{COLONY}/actions/runs/{run['id']}\n",
        'tree': tree['sha'], 'parents': [base],
        'author': {'name': 'github-actions[bot]', 'email': '41898282+github-actions[bot]@users.noreply.github.com'}})
    # No force/rebase/merge: a concurrent main change rejects this update.
    api.request(f'repos/{COLONY}/git/refs/heads/main', 'PATCH', {'sha': commit['sha'], 'force': False})
    print('Published validated Antfly pin:', target, 'Colony commit:', commit['sha'])
    return commit['sha']


def main():
    request = os.environ['SYNC_REQUEST']
    require(re.fullmatch(r'[0-9]+-[0-9]+', request), 'Invalid request identifier')
    api = GitHub(os.environ['COLONY_DISPATCH_TOKEN'])
    run, manifest = wait_for_validation(api, request)
    publish(api, run, manifest)


if __name__ == '__main__':
    try:
        main()
    except PublicationError as exc:
        raise SystemExit('Automatic pin publication stopped: '+str(exc)) from None
    except (ValueError, KeyError, OSError, zipfile.BadZipFile):
        # API bodies/redirect URLs and private repository contents stay out of public logs.
        raise SystemExit('Automatic pin publication stopped; inspect private Colony validation and protected-main permissions. No forced update or protection change was attempted.')
