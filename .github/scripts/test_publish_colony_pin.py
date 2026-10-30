import io
import json
import unittest
from unittest.mock import patch
import urllib.error
import zipfile

import publish_colony_pin as p

BASE, OLD, TARGET, TREE, NEW_TREE, COMMIT = (str(i) * 40 for i in range(1, 7))


def zipped(value, name="pin.json"):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(name, json.dumps(value))
    return stream.getvalue()


class FakeAPI:
    def __init__(self):
        self.writes = []
        self.manifest = {
            "version": 1,
            "request": "10-1",
            "base_sha": BASE,
            "old_sha": OLD,
            "target_sha": TARGET,
            "changed": True,
        }
        self.run = {
            "id": 12,
            "display_title": "Antfly pin validation 10-1",
            "event": "repository_dispatch",
            "head_branch": "main",
            "head_repository": {"full_name": p.COLONY},
            "head_sha": BASE,
            "status": "completed",
            "conclusion": "success",
        }
        self.main = BASE
        self.forward, self.reachable = "ahead", "identical"
        self.corrupt_tree = False
        self.rows = [
            {"path": "antfly", "mode": "160000", "type": "commit", "sha": OLD},
            {"path": ".github", "mode": "040000", "type": "tree", "sha": "f" * 40},
        ]
        self.push_rejected = False

    def request(self, path, method="GET", body=None):
        if method != "GET":
            self.writes.append((path, method, body))
        if "/actions/workflows/" in path:
            return {"workflow_runs": [self.run]}
        if path.endswith("/artifacts"):
            return {
                "artifacts": [{"id": 13, "name": "antfly-pin-10-1", "expired": False}]
            }
        if path.endswith("/git/ref/heads/main"):
            return {"object": {"sha": self.main}}
        if path.endswith(f"/compare/{OLD}...{OLD}"):
            return {"status": "identical"}
        if path.endswith(f"/compare/{OLD}...main"):
            return {"status": self.reachable}
        if path.endswith(f"/compare/{OLD}...{TARGET}"):
            return {"status": self.forward}
        if path.endswith(f"/compare/{TARGET}...main"):
            return {"status": self.reachable}
        if path.endswith("/git/commits/" + BASE):
            return {"tree": {"sha": TREE}}
        if path.endswith("/git/trees/" + TREE):
            return {"tree": self.rows}
        if path.endswith("/git/trees"):
            rows = [
                dict(row, sha=TARGET) if row["path"] == "antfly" else dict(row)
                for row in self.rows
            ]
            if self.corrupt_tree:
                rows[1]["sha"] = "0" * 40
            return {"sha": NEW_TREE, "tree": rows}
        if path.endswith("/git/commits"):
            return {"sha": COMMIT}
        if path.endswith("/git/refs/heads/main"):
            if self.push_rejected:
                raise ValueError("Non-fast-forward")
            return {"object": {"sha": COMMIT}}
        raise AssertionError(path)

    def archive(self, artifact_id):
        assert artifact_id == 13
        return zipped(self.manifest)


class PublisherTests(unittest.TestCase):
    def test_success_writes_only_gitlink_and_records_validation(self):
        api = FakeAPI()
        run, manifest = p.wait_for_validation(api, "10-1", timeout=1)
        self.assertEqual(p.publish(api, run, manifest), COMMIT)
        tree, commit, update = [row[2] for row in api.writes]
        self.assertEqual(
            tree["tree"],
            [{"path": "antfly", "mode": "160000", "type": "commit", "sha": TARGET}],
        )
        self.assertEqual(commit["parents"], [BASE])
        self.assertIn(
            "Validated-Run: https://github.com/antflydb/colony/actions/runs/12",
            commit["message"],
        )
        self.assertEqual(update, {"sha": COMMIT, "force": False})

    def test_failed_cancelled_wrong_branch_or_repository_cannot_publish(self):
        for key, value in [
            ("conclusion", "failure"),
            ("conclusion", "cancelled"),
            ("head_branch", "feature"),
            ("head_repository", {"full_name": "other/repo"}),
        ]:
            api = FakeAPI()
            api.run[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                p.wait_for_validation(api, "10-1", timeout=1)
            self.assertFalse(api.writes)

    def test_manifest_must_match_request_and_validated_base(self):
        for key, value in [("request", "99-1"), ("base_sha", "a" * 40)]:
            api = FakeAPI()
            api.manifest[key] = value
            with self.assertRaises(ValueError):
                p.wait_for_validation(api, "10-1", timeout=1)
            self.assertFalse(api.writes)

    def test_stale_base_or_rewind_stops_before_writing(self):
        for field, value in [
            ("main", "a" * 40),
            ("forward", "behind"),
            ("reachable", "diverged"),
        ]:
            api = FakeAPI()
            setattr(api, field, value)
            with self.subTest(field=field), self.assertRaises(ValueError):
                p.publish(api, api.run, api.manifest)
            self.assertFalse(api.writes)

    def test_root_tree_must_preserve_every_other_entry(self):
        api = FakeAPI()
        api.corrupt_tree = True
        with self.assertRaises(ValueError):
            p.publish(api, api.run, api.manifest)
        self.assertEqual(
            len(api.writes), 1, "No commit/ref may be written after a mismatched tree"
        )

    def test_noop_does_not_commit(self):
        api = FakeAPI()
        api.manifest.update(target_sha=OLD, changed=False)
        self.assertIsNone(p.publish(api, api.run, api.manifest))
        self.assertFalse(api.writes)

    def test_concurrent_push_is_never_forced_or_retried(self):
        api = FakeAPI()
        api.push_rejected = True
        with self.assertRaises(ValueError):
            p.publish(api, api.run, api.manifest)
        updates = [body for path, method, body in api.writes if method == "PATCH"]
        self.assertEqual(updates, [{"sha": COMMIT, "force": False}])

    def test_archive_rejects_extra_files_invalid_shas_and_inconsistent_flags(self):
        api = FakeAPI()
        for key, value in [("target_sha", "bad"), ("changed", False)]:
            manifest = dict(api.manifest)
            manifest[key] = value
            with self.assertRaises(ValueError):
                p.read_manifest(zipped(manifest))
        with self.assertRaises(ValueError):
            p.read_manifest(zipped(api.manifest, "../pin.json"))

    def test_archive_redirect_never_receives_the_pat(self):
        api = p.GitHub("SENTINEL_TOKEN")
        error = urllib.error.HTTPError(
            "https://api.github.com/artifact",
            302,
            "Found",
            {"Location": "https://storage.example/opaque-signed-url"},
            None,
        )
        with (
            patch.object(api.opener, "open", side_effect=error) as first,
            patch.object(
                p.urllib.request, "urlopen", return_value=io.BytesIO(b"archive")
            ) as second,
        ):
            self.assertEqual(api.archive(13), b"archive")
        self.assertEqual(
            first.call_args.args[0].get_header("Authorization"), "Bearer SENTINEL_TOKEN"
        )
        self.assertEqual(
            second.call_args.args, ("https://storage.example/opaque-signed-url",)
        )
        self.assertNotIn("SENTINEL_TOKEN", str(second.call_args))


if __name__ == "__main__":
    unittest.main()
