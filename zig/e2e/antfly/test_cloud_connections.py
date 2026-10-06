# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Local cloud credential commands use isolated vendor tools, never real grants."""

import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import requests
from conftest import DEFAULT_ANTFLY_BIN, resolve_binary_path
from port_reservations import find_free_port


@pytest.fixture
def cloud_cli(tmp_path):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).is_file():
        pytest.skip("Antfly binary unavailable")
    tools = tmp_path / "tools"
    tools.mkdir()
    log = tmp_path / "calls.jsonl"
    config = tmp_path / "aws-config"
    config.write_text(
        "[profile work]\nlogin_session = test-console\n[profile company]\nsso_session = test-sso\n"
    )
    # Each child receives its own environment; global credentials remain untouched.
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("AWS_", "GOOGLE_", "GCLOUD_", "ANTFLY_", "CLOUDSDK_"))
    }
    env.update(
        PATH=str(tools),
        HOME=str(tmp_path),
        AWS_CONFIG_FILE=str(config),
        AWS_SHARED_CREDENTIALS_FILE=str(tmp_path / "aws-credentials"),
        CLOUDSDK_CONFIG=str(tmp_path / "gcloud"),
        CLOUD_TEST_LOG=str(log),
        ANTFLY_URL="not-a-valid-url",
        ANTFLY_TOKEN="conflicting",
        ANTFLY_USERNAME="unused",
        ANTFLY_PASSWORD="unused",
    )
    script = (
        "#!"
        + sys.executable
        + "\n"
        + r"""import json,os,sys
from pathlib import Path
args=sys.argv[1:]
with open(os.environ["CLOUD_TEST_LOG"],"a") as out:
 out.write(json.dumps([Path(sys.argv[0]).name]+args)+"\n")
if args[:2]==["configure","export-credentials"]:
 if os.environ.get("CLOUD_TEST_FAILURE") or (os.environ.get("CLOUD_TEST_FAILURE_FILE") and Path(os.environ["CLOUD_TEST_FAILURE_FILE"]).exists()):
  print("secret-vendor-diagnostic",file=sys.stderr);sys.exit(1)
 value={"Version":1,"AccessKeyId":"fake-access","SecretAccessKey":"fake-secret","SessionToken":"fake-session","Expiration":os.environ.get("CLOUD_TEST_EXPIRATION","2035-01-01T00:00:00+00:00")}
 for key in os.environ.get("CLOUD_TEST_OMIT", "").split(","): value.pop(key, None)
 print(json.dumps(value))
elif args[:3]==["auth","application-default","print-access-token"]:
 print("fake-google-token")
"""
    )
    for name in ("aws", "gcloud"):
        tool = tools / name
        tool.write_text(script)
        tool.chmod(0o755)

    def run(*args, extra_env=None):
        child_env = env | (extra_env or {})
        return subprocess.run(
            [binary, "connections", *args],
            env=child_env,
            capture_output=True,
            check=False,
            text=True,
            timeout=15,
        )

    def calls():
        return (
            [json.loads(line) for line in log.read_text().splitlines()]
            if log.exists()
            else []
        )

    return run, calls, env


def assert_safe(result, status="available"):
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == status
    for secret in (
        "fake-secret",
        "fake-session",
        "fake-google-token",
        "secret-vendor-diagnostic",
    ):
        assert secret not in result.stdout + result.stderr


def test_google_adc_login_list_logout_without_antfly_server(cloud_cli):
    run, calls, _ = cloud_cli
    assert_safe(run("login", "google", "--project", "my-project", "--no-browser"))
    assert calls()[0] == [
        "gcloud",
        "auth",
        "application-default",
        "login",
        "--scopes=openid,https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/cloud-platform",
        "--project",
        "my-project",
        "--no-launch-browser",
    ]
    assert_safe(run("list", "google"))
    assert_safe(run("logout", "google"), "disconnected")
    assert calls()[-1] == ["gcloud", "auth", "application-default", "revoke", "--quiet"]


def test_aws_console_profile_login_list_logout_without_antfly_server(cloud_cli):
    run, calls, _ = cloud_cli
    assert_safe(run("login", "aws", "--profile", "work"))
    assert calls()[0] == ["aws", "login", "--profile", "work", "--no-cli-pager"]
    assert calls()[1] == [
        "aws",
        "configure",
        "export-credentials",
        "--profile",
        "work",
        "--format",
        "process",
        "--no-cli-pager",
        "--no-cli-auto-prompt",
    ]
    assert_safe(run("list", "aws", "--profile", "work"))
    assert_safe(run("logout", "aws", "--profile", "work"), "disconnected")
    assert calls()[-1] == ["aws", "logout", "--profile", "work", "--no-cli-pager"]


def test_aws_sso_selected_by_profile_and_global_logout_rejected(cloud_cli):
    run, calls, _ = cloud_cli
    assert_safe(run("login", "aws", "--profile", "company", "--no-browser"))
    assert calls()[0] == [
        "aws",
        "sso",
        "login",
        "--profile",
        "company",
        "--no-cli-pager",
        "--no-browser",
    ]
    before = calls()
    result = run("logout", "aws", "--profile", "company")
    assert result.returncode != 0 and "AwsSsoLogoutIsGlobal" in result.stderr
    assert calls() == before


@pytest.mark.parametrize(
    "args",
    [
        ("login", "aws"),
        ("login", "aws", "--profile", "work", "--no-browser"),
        ("logout", "aws", "--profile", "static"),
        ("login", "google", "--profile", "work"),
    ],
)
def test_cloud_invalid_options_do_not_launch_vendor_tools(cloud_cli, args):
    run, calls, _ = cloud_cli
    assert run(*args).returncode != 0
    assert calls() == []


def test_google_explicit_credentials_override_is_rejected(cloud_cli):
    run, calls, _ = cloud_cli
    result = run(
        "login", "google", extra_env={"GOOGLE_APPLICATION_CREDENTIALS": "/unused.json"}
    )
    assert result.returncode != 0 and "GoogleAdcOverrideActive" in result.stderr
    assert calls() == []


@pytest.mark.parametrize(
    "extra_env",
    [
        {"CLOUD_TEST_FAILURE": "1"},
        {"CLOUD_TEST_EXPIRATION": "2000-01-01T00:00:00Z"},
        {"CLOUD_TEST_EXPIRATION": "malformed"},
    ],
)
def test_aws_export_failure_or_expiration_never_reports_available(cloud_cli, extra_env):
    run, _, _ = cloud_cli
    result = run("list", "aws", "--profile", "work", extra_env=extra_env)
    assert result.returncode != 0
    assert "available" not in result.stdout
    for secret in ("fake-secret", "fake-session", "secret-vendor-diagnostic"):
        assert secret not in result.stdout + result.stderr


@pytest.mark.parametrize("kind", ["sso", "console"])
def test_aws_role_login_authenticates_source_but_exports_selected_role(cloud_cli, kind):
    run, calls, env = cloud_cli
    marker = "sso_session" if kind == "sso" else "login_session"
    Path(env["AWS_CONFIG_FILE"]).write_text(
        "[profile work]\nrole_arn=arn:role/work\nsource_profile=middle\n"
        "[profile middle]\nrole_arn=arn:role/middle\nsource_profile=company\n"
        f"[profile company]\n{marker}=test-session\n"
    )
    assert_safe(run("login", "aws", "--profile", "work"))
    expected = ["aws"] + (["sso"] if kind == "sso" else [])
    assert calls()[0] == expected + ["login", "--profile", "company", "--no-cli-pager"]
    assert calls()[1][4] == "work"
    if kind == "console":
        assert_safe(run("logout", "aws", "--profile", "work"), "disconnected")
        assert calls()[-1] == [
            "aws",
            "logout",
            "--profile",
            "company",
            "--no-cli-pager",
        ]
    else:
        before = calls()
        result = run("logout", "aws", "--profile", "work")
        assert result.returncode != 0 and "AwsSsoLogoutIsGlobal" in result.stderr
        assert calls() == before


@pytest.mark.parametrize("operation", ["login", "list"])
@pytest.mark.parametrize(
    "omit", ["SessionToken", "Expiration", "SessionToken,Expiration"]
)
def test_aws_browser_readiness_rejects_incomplete_temporary_exports(
    cloud_cli, operation, omit
):
    run, _, _ = cloud_cli
    result = run(
        operation, "aws", "--profile", "work", extra_env={"CLOUD_TEST_OMIT": omit}
    )
    assert result.returncode != 0
    assert "available" not in result.stdout
    assert "InvalidAwsCredentialExport" in result.stderr
    assert "fake-secret" not in result.stdout + result.stderr


def test_aws_static_profile_list_remains_available(cloud_cli):
    run, _, env = cloud_cli
    Path(env["AWS_CONFIG_FILE"]).write_text("[profile work]\nregion=us-east-1\n")
    assert_safe(
        run(
            "list",
            "aws",
            "--profile",
            "work",
            extra_env={"CLOUD_TEST_OMIT": "SessionToken,Expiration"},
        )
    )


def test_aws_cycle_fails_before_launching_vendor_tools(cloud_cli):
    run, calls, env = cloud_cli
    Path(env["AWS_CONFIG_FILE"]).write_text(
        "[profile work]\nrole_arn=arn:role/work\nsource_profile=other\n[profile other]\nrole_arn=arn:role/other\nsource_profile=work\n"
    )
    result = run("list", "aws", "--profile", "work")
    assert result.returncode != 0 and "InvalidAwsProfileChain" in result.stderr
    assert calls() == []


@pytest.mark.parametrize("source", ["profile", "default"])
@pytest.mark.parametrize(
    "profile_kind", ["console", "sso_role", "console_role", "static_role"]
)
def test_s3_browser_profiles_sign_requests_and_fail_closed(
    cloud_cli, tmp_path, source, profile_kind
):
    _, calls, env = cloud_cli
    if profile_kind != "console":
        marker = {
            "sso_role": "sso_session=test-sso",
            "console_role": "login_session=test-console",
            "static_role": "region=us-east-1",
        }[profile_kind]
        Path(env["AWS_CONFIG_FILE"]).write_text(
            "[profile work]\nrole_arn=arn:role/work\nsource_profile=company\n"
            f"[profile company]\n{marker}\n"
        )
    failure = tmp_path / "export-failed"
    env = env | {"AWS_PROFILE": "work", "CLOUD_TEST_FAILURE_FILE": str(failure)}
    # A stale static file and a metadata endpoint must never substitute for
    # the selected browser account when credential export fails.
    Path(env["AWS_SHARED_CREDENTIALS_FILE"]).write_text(
        "[work]\naws_access_key_id = stale-static\naws_secret_access_key = stale-secret\n"
    )
    signed = []

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            signed.append(dict(self.headers))
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    storage = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=storage.serve_forever, daemon=True)
    thread.start()
    port = find_free_port()
    credentials = {"source": source}
    if source == "profile":
        credentials["profile"] = "work"
    config = tmp_path / "node.json"
    config.write_text(
        json.dumps(
            {
                "connectors": {"chatgpt": {"enabled": False}},
                "connections": {
                    "archive": {
                        "kind": "external_io",
                        "capabilities": ["objects.read"],
                        "external_io": {
                            "protocol": "s3",
                            "endpoint": f"http://127.0.0.1:{storage.server_port}",
                            "region": "us-east-1",
                            "use_ssl": False,
                            "addressing_style": "path",
                            "buckets": ["archive"],
                            "credentials": credentials,
                        },
                    }
                },
            }
        )
    )
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    log = tmp_path / "server.log"
    with log.open("w") as out:
        proc = subprocess.Popen(
            [
                binary,
                "standalone",
                "--config",
                str(config),
                "--port",
                str(port),
                "--health",
                "false",
                "--auth",
                "false",
                "--data-dir",
                str(tmp_path / "data"),
                "--models-dir",
                str(tmp_path / "models"),
                "--ml-dir",
                str(tmp_path / "ml"),
            ],
            env=env,
            stdout=out,
            stderr=out,
        )
        try:
            url = f"http://127.0.0.1:{port}/db/v1/connections"
            deadline = time.monotonic() + 45
            while True:
                try:
                    response = requests.get(url, timeout=1)
                    if response.status_code == 200:
                        break
                except requests.RequestException:
                    pass
                assert proc.poll() is None and time.monotonic() < deadline, (
                    log.read_text()
                )
                time.sleep(0.1)

            def probe():
                result = requests.get(
                    url, params={"include": "status", "refresh": "true"}, timeout=15
                )
                result.raise_for_status()
                return next(
                    c for c in result.json()["connections"] if c["id"] == "archive"
                )

            first = probe()
            assert first["status"] == "connected", first
            assert signed
            for headers in signed:
                assert "Credential=fake-access/" in headers["Authorization"]
                assert headers["x-amz-security-token"] == "fake-session"
            exports = [
                call
                for call in calls()
                if call[:3] == ["aws", "configure", "export-credentials"]
            ]
            assert exports and "work" in exports[-1]
            before = len(signed)
            failure.touch()
            failed = probe()
            assert failed["status"] == "error", failed
            assert len(signed) == before
            assert "fake-secret" not in json.dumps(failed)
            assert "secret-vendor-diagnostic" not in json.dumps(failed)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            storage.shutdown()
            storage.server_close()
            thread.join(timeout=5)
