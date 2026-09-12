"""Offline contracts for the opt-in V2 extraction monitoring artifacts.

The default suite uses only the Python standard library. Set
ANTFLY_GLINER25_PROMTOOL to a separately verified official promtool 3.14.0
binary to also parse/evaluate the real PromQL rules, fixtures and dashboard.
No command starts a server, discovers targets, installs tools or loads models.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[5]
ARTIFACTS = ROOT / "devops/monitoring/gliner25"
METRICS = ROOT / "zig/pkg/inference/src/server/extraction_metrics.zig"
PREFIX = "antfly_inference_extract_v2_"
RECORD_PREFIX = "antfly_gliner25_instance:"
OPT_IN = 'gliner25_monitor="enabled"'
PROMTOOL = os.environ.get("ANTFLY_GLINER25_PROMTOOL")


def read_json(name: str):
    path = ARTIFACTS / name
    if path.stat().st_size > 256 * 1024:
        raise AssertionError(f"Monitoring artifact is unexpectedly large: {name}")
    return json.loads(path.read_text())


def rules(name: str):
    return [rule for group in read_json(name)["groups"] for rule in group["rules"]]


def dashboard_queries():
    return [target["expr"] for panel in read_json("dashboard.json")["panels"]
            for target in panel.get("targets", [])]


class MonitoringContracts(unittest.TestCase):
    def test_dashboard_queries_are_bounded_per_worker_and_preserve_missing_data(self):
        dashboard = read_json("dashboard.json")
        self.assertLessEqual(len(dashboard["panels"]), 24)
        self.assertEqual(dashboard["refresh"], "30s")
        self.assertEqual(len({p["id"] for p in dashboard["panels"]}), len(dashboard["panels"]))
        self.assertEqual({v["name"] for v in dashboard["templating"]["list"]},
                         {"DS_PROMETHEUS", "job", "instance"})
        for panel in dashboard["panels"]:
            self.assertNotIn("repeat", panel)
            if not panel.get("targets"):
                continue
            self.assertLessEqual(panel["maxDataPoints"], 1000)
            self.assertEqual(panel["fieldConfig"]["defaults"]["noValue"], "NO DATA")
            self.assertFalse(panel["fieldConfig"]["defaults"]["custom"]["spanNulls"])
            for target in panel["targets"]:
                expr = target["expr"]
                with self.subTest(panel=panel["title"], expression=expr):
                    for matcher in (OPT_IN, 'job=~"$job"', 'instance=~"$instance"'):
                        self.assertIn(matcher, expr)
                    self.assertNotRegex(expr, r"\bor\s+(?:on\s*\([^)]*\)\s*)?vector\s*\(")
                    self.assertNotIn("fill(", expr)
                    self.assertNotRegex(expr, r"rate\(\s*(?:sum|avg|max|min)\s*\(")
                    for grouping in re.findall(r"\bby\s*\(([^)]+)\)", expr):
                        self.assertTrue({"job", "instance"}.issubset(
                            {label.strip() for label in grouping.split(",")}))

    def test_every_metric_selector_matches_the_current_native_renderer(self):
        source = METRICS.read_text()
        names = set(re.findall(r'"(antfly_inference_extract(?:_v2)?_[a-z_]+)"', source))
        names.update(re.findall(r"# HELP (antfly_inference_extract(?:_v2)?_[a-z_]+) ", source))
        names.update(PREFIX + "duration_ns_" + part for part in ("bucket", "sum", "count"))
        expressions = dashboard_queries() + [r["expr"] for file in
                      ("recording.rules.json", "alerts.rules.json") for r in rules(file)]
        selected = set()
        for expr in expressions:
            selected.update(re.findall(r"\b(antfly_inference_extract(?:_v2)?_[a-z_]+)\{", expr))
        self.assertTrue(selected)
        self.assertEqual(selected - names, set())
        for variable in read_json("dashboard.json")["templating"]["list"]:
            if variable["type"] == "query":
                self.assertIn("label_values(up{", variable["definition"])

    def test_units_atomic_output_and_solver_families_are_not_conflated(self):
        panels = read_json("dashboard.json")["panels"]
        for panel in panels:
            for target in panel.get("targets", []):
                expr = target["expr"]
                if "duration_ns" in expr:
                    self.assertIn("/ 1e9", expr)
                    self.assertEqual(panel["fieldConfig"]["defaults"]["unit"], "s")
                if "host_peak_bytes_max" in expr:
                    self.assertNotIn("rate(", expr)
                    self.assertEqual(panel["fieldConfig"]["defaults"]["unit"], "bytes")
                if "solver_exhausted_total" in expr:
                    self.assertNotIn("search_exhausted", expr)
                if "returned_items_total" in expr:
                    self.assertNotIn("decoded_items_total", expr)
        record = {r["record"]: r["expr"] for r in rules("recording.rules.json")}
        server = record[RECORD_PREFIX + "server_failure_ratio:rate5m"]
        self.assertIn('outcome=~"backing_oom|model_error|internal"', server)
        for caller_outcome in ("invalid", "unsupported", "cancelled", "infeasible", "memory_budget"):
            self.assertNotIn(caller_outcome, server)
        self.assertIn('outcome="search_exhausted"',
                      record[RECORD_PREFIX + "strict_exhaustion_ratio:rate5m"])
        self.assertIn("solver_exhausted_total",
                      record[RECORD_PREFIX + "witness_exhaustions:rate5m"])

    def test_recording_rules_keep_opt_in_worker_identity_and_reset_semantics(self):
        document = read_json("recording.rules.json")
        self.assertLessEqual(sum(len(g["rules"]) for g in document["groups"]), 20)
        for group in document["groups"]:
            self.assertLessEqual(group["limit"], 10000)
            for rule in group["rules"]:
                expr = rule["expr"]
                self.assertIn(OPT_IN, expr)
                self.assertNotRegex(expr, r"(?:rate|increase)\(\s*(?:sum|avg|max|min)\s*\(")
                self.assertNotIn("vector(0)", expr)
                for grouping in re.findall(r"\bby\s*\(([^)]+)\)", expr):
                    self.assertTrue({"job", "instance"}.issubset(
                        {label.strip() for label in grouping.split(",")}))

    def test_alerts_are_opt_in_nonpaging_and_have_positive_negative_native_cases(self):
        alerts = rules("alerts.rules.json")
        tests = read_json("rules.test.json")["tests"]
        self.assertLessEqual(len(alerts), 8)
        for alert in alerts:
            self.assertIn(alert["labels"]["severity"], {"warning", "info"})
            self.assertTrue(alert["for"])
            if OPT_IN not in alert["expr"]:
                self.assertIn(RECORD_PREFIX, alert["expr"])
                self.assertIn("completions:increase5m >= 20", alert["expr"])
            cases = [c for test in tests for c in test.get("alert_rule_test", [])
                     if c["alertname"] == alert["alert"]]
            self.assertTrue(any(c["exp_alerts"] for c in cases), alert["alert"])
            self.assertTrue(any(not c["exp_alerts"] for c in cases), alert["alert"])
            self.assertIn("work-log/completed/gliner2.5.md#", alert["annotations"]["runbook"])
        # Loading the examples does not configure an Alertmanager or receiver.
        self.assertNotIn("alerting", read_json("prometheus.example.json"))

    def test_example_config_and_compose_overlay_do_not_change_default_deployment(self):
        config = read_json("prometheus.example.json")
        self.assertEqual(config["rule_files"], ["recording.rules.json", "alerts.rules.json"])
        scrape, = config["scrape_configs"]
        self.assertEqual(scrape["metrics_path"], "/ml/v1/metrics")
        target, = scrape["static_configs"]
        self.assertEqual(target["labels"], {"gliner25_monitor": "enabled"})
        self.assertTrue(all(".invalid:" in name for name in target["targets"]))
        self.assertLessEqual(scrape["sample_limit"], 20000)
        overlay = read_json("compose.dashboard.json")
        self.assertEqual(set(overlay["services"]), {"grafana"})
        volume, = overlay["services"]["grafana"]["volumes"]
        self.assertTrue(volume["read_only"])
        for variant in ("docker-compose", "docker-compose-s3"):
            self.assertEqual((ROOT / "devops" / variant / volume["source"]).resolve(),
                             (ARTIFACTS / "dashboard.json").resolve())
        self.assertNotIn("gliner25", (ROOT / "devops/prometheus.yml").read_text())


@unittest.skipUnless(PROMTOOL, "Set ANTFLY_GLINER25_PROMTOOL to a verified official promtool 3.14.0")
class MonitoringPromtool(unittest.TestCase):
    def run_tool(self, *args):
        return subprocess.run([PROMTOOL, *args], cwd=ARTIFACTS, check=True,
                              capture_output=True, text=True, timeout=30,
                              env=dict(os.environ, GOMAXPROCS="1"))

    def test_native_config_rules_and_semantic_fixtures(self):
        self.assertIn("version 3.14.0", self.run_tool("--version").stdout)
        self.run_tool("check", "config", "prometheus.example.json")
        self.run_tool("check", "rules", "recording.rules.json", "alerts.rules.json")
        self.run_tool("test", "rules", "rules.test.json")

    def test_all_dashboard_promql_parses_without_starting_a_server(self):
        # Promtool's ordinary rule checker parses the expressions; substitute
        # only Grafana's three query variables with bounded fixture values.
        queries = [expr.replace("$__rate_interval", "5m").replace("$job", "test")
                   .replace("$instance", "worker") for expr in dashboard_queries()]
        payload = {"groups": [{"name": "dashboard-parser", "rules": [
            {"record": f"dashboard_check_{index}", "expr": expression}
            for index, expression in enumerate(queries)]}]}
        with tempfile.TemporaryDirectory(prefix="antfly-gliner25-promql-") as tmp:
            file = Path(tmp) / "dashboard.rules.json"
            file.write_text(json.dumps(payload))
            self.run_tool("check", "rules", str(file))


if __name__ == "__main__":
    unittest.main()
