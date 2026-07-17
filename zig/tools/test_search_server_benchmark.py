#!/usr/bin/env python3

import importlib.util
import json
import struct
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


SCRIPT = Path(__file__).with_name("run_search_server_benchmark.py")
SPEC = importlib.util.spec_from_file_location("server_benchmark", SCRIPT)
assert SPEC and SPEC.loader
benchmark = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = benchmark
SPEC.loader.exec_module(benchmark)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            json.loads(self.rfile.read(length))
        payload = b'{"hits":[{"id":"doc:1","score":1.0}]}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_):
        pass


class ServerBenchmarkTest(unittest.TestCase):
    def test_parse_docker_memory_units(self):
        self.assertEqual(128, benchmark.parse_byte_size("128B"))
        self.assertEqual(1572864, benchmark.parse_byte_size("1.5MiB"))
        self.assertEqual(2500000000, benchmark.parse_byte_size("2.5GB"))

    def test_index_command_reports_loader_and_server_resources(self):
        elapsed, server, loader = benchmark.run_index_command("/usr/bin/true", None, None)
        self.assertGreater(elapsed, 0)
        self.assertEqual(
            {
                "peak_rss_bytes": None,
                "cpu_percent_mean": None,
                "cpu_percent_max": None,
                "metrics_latest": {},
                "metrics_peak": {},
                "metrics_at_peak_footprint": {},
                "metrics_at_peak_rss": {},
                "metrics_at_peak_malloc": {},
                "peak_footprint_sample_seconds": None,
                "peak_rss_sample_seconds": None,
                "peak_malloc_sample_seconds": None,
                "disk": None,
            },
            server,
        )
        self.assertGreaterEqual(loader["cpu_seconds"], 0)
        self.assertGreaterEqual(loader["cpu_utilization"], 0)

    def test_process_sampler_retains_coherent_peak_metric_snapshots(self):
        sampler = benchmark.ProcessSampler(None, metrics_url="http://unused")
        sampler.observe_metrics(
            "\n".join(
                (
                    "antfly_process_footprint_bytes 100",
                    "antfly_process_resident_bytes 200",
                    "antfly_process_malloc_allocated_bytes 80",
                    'antfly_resource_used_bytes{slice="lsm.in_memory_state"} 60',
                    "unrelated_metric 999",
                )
            )
        )
        sampler.observe_metrics(
            "\n".join(
                (
                    "antfly_process_footprint_bytes 150",
                    "antfly_process_resident_bytes 180",
                    "antfly_process_malloc_allocated_bytes 70",
                    'antfly_resource_used_bytes{slice="lsm.in_memory_state"} 40',
                )
            )
        )
        sampler.observe_metrics(
            "\n".join(
                (
                    "antfly_process_footprint_bytes 120",
                    "antfly_process_resident_bytes 250",
                    "antfly_process_malloc_allocated_bytes 90",
                    'antfly_resource_used_bytes{slice="lsm.in_memory_state"} 30',
                )
            )
        )
        summary = sampler.summary()
        self.assertEqual(
            40,
            summary["metrics_at_peak_footprint"]['antfly_resource_used_bytes{slice="lsm.in_memory_state"}'],
        )
        self.assertEqual(
            30,
            summary["metrics_at_peak_rss"]['antfly_resource_used_bytes{slice="lsm.in_memory_state"}'],
        )
        self.assertEqual(150, summary["metrics_peak"]["antfly_process_footprint_bytes"])
        self.assertNotIn("unrelated_metric", summary["metrics_latest"])

    def test_directory_inventory_attributes_subtrees_and_files(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            table = root / "data" / "replicas" / "group-1" / "table-db"
            (table / "indexes" / "text" / "segments").mkdir(parents=True)
            (table / "runs").mkdir()
            (table / "indexes" / "text" / "segments" / "1.seg").write_bytes(b"x" * 7)
            (table / "runs" / "1.tbl").write_bytes(b"y" * 3)
            inventory = benchmark.directory_inventory(root)
        self.assertEqual(10, inventory["total_bytes"])
        self.assertEqual({"files": 1, "bytes": 3}, inventory["storage_categories"]["primary_runs"])
        self.assertEqual({"files": 1, "bytes": 7}, inventory["storage_categories"]["text_segments"])
        self.assertEqual(
            {"path": "data/replicas/group-1/table-db/indexes/text/segments/1.seg", "bytes": 7},
            inventory["largest_files"][0],
        )

    def test_disk_sampler_tracks_peak_categories(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            runs = root / "data" / "replicas" / "group-1" / "table-db" / "runs"
            runs.mkdir(parents=True)
            sampler = benchmark.DiskSampler(root)
            (runs / "1.tbl").write_bytes(b"a" * 3)
            sampler._sample()
            (runs / "2.tbl").write_bytes(b"b" * 5)
            sampler._sample()
            summary = sampler.summary()
        self.assertEqual(8, summary["peak_total_bytes"])
        self.assertEqual({"files": 2, "bytes": 8}, summary["peak_storage_categories"]["primary_runs"])

    def test_directory_inventory_reconciles_lsm_manifest_generations(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            table = root / "data" / "replicas" / "group-1" / "table-db"
            runs = table / "runs"
            runs.mkdir(parents=True)
            active = runs / "1.tbl"
            obsolete = runs / "2.tbl"
            untracked = runs / "3.tbl"
            active.write_bytes(b"a" * 11)
            obsolete.write_bytes(b"b" * 13)
            untracked.write_bytes(b"c" * 17)
            table.joinpath("manifest.bin").write_bytes(
                lsm_manifest([active], [obsolete], active_size_bytes=10)
            )
            inventory = benchmark.directory_inventory(root)
        manifest = inventory["lsm_manifests"][0]
        self.assertEqual(
            {
                "files": 1,
                "bytes": 11,
                "missing": 0,
                "logical_bytes": 10,
                "logical_entry_bytes": 0,
                "physical_entry_bytes": 0,
                "table_overhead_bytes": 11,
                "raw_blocks": 0,
                "compressed_blocks": 0,
                "compression_codec_mask": 0,
            },
            manifest["active"],
        )
        self.assertEqual(
            {
                "id": 1,
                "level": 0,
                "size_bytes": 10,
                "entry_count": 1,
                "logical_entry_bytes": 0,
                "physical_entry_bytes": 0,
                "raw_blocks": 0,
                "compressed_blocks": 0,
                "compression_codec_mask": 0,
                "smallest_namespace_hex": "",
                "smallest_key_hex": "61",
                "largest_namespace_hex": "",
                "largest_key_hex": "7a",
                "partition_prefix_equal": False,
            },
            manifest["active_runs"][0],
        )
        self.assertEqual({"files": 1, "bytes": 13, "missing": 0}, manifest["obsolete"])
        self.assertEqual({"files": 3, "bytes": 41, "missing": 0}, manifest["physical"])
        self.assertEqual({"files": 1, "bytes": 17, "missing": 0}, manifest["untracked"])
    def test_freshness_requires_marker_in_expectation(self):
        self.assertTrue(benchmark.template_has_marker({"expect_contains": "{marker}"}))
        self.assertFalse(benchmark.template_has_marker({"body": {"query": "constant"}}))

    def test_response_hit_count_supports_antfly_and_quickwit(self):
        self.assertEqual(1, benchmark.response_hit_count(b'{"hits":[{"id":"doc:1"}]}'))
        self.assertEqual(
            2,
            benchmark.response_hit_count(
                b'{"responses":[{"hits":{"hits":[{"_id":"doc:1"},{"_id":"doc:2"}]}}]}'
            ),
        )
        self.assertEqual(0, benchmark.response_hit_count(b'{"hits":[]}'))
        self.assertIsNone(benchmark.response_hit_count(b'not-json'))

    def test_client_rejects_empty_search_results_when_required(self):
        class EmptyHandler(Handler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                if length:
                    self.rfile.read(length)
                payload = b'{"hits":[]}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        server = ThreadingHTTPServer(("127.0.0.1", 0), EmptyHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = benchmark.Client(f"http://127.0.0.1:{server.server_port}", 2)
            observation = client.request(
                {"method": "POST", "path": "/search", "expect_nonempty_hits": True},
                benchmark.time.perf_counter_ns(),
                "search",
            )
            client.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
        self.assertEqual("search returned no hits", observation.error)

    def test_substitute_replaces_marker_in_object_keys(self):
        self.assertEqual(
            {"freshness:abc": {"body": "abc"}},
            benchmark.substitute({"freshness:{marker}": {"body": "{marker}"}}, "abc"),
        )

    def test_open_loop_records_success_and_coordinated_latency(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            observations = benchmark.run_open_loop(
                f"http://127.0.0.1:{server.server_port}",
                2,
                {"method": "POST", "path": "/search", "body": {"query": "alpha"}},
                None,
                0,
                2,
                20,
                0.2,
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
        self.assertEqual(4, len(observations))
        self.assertTrue(all(observation.error is None for observation in observations))
        self.assertTrue(all(observation.end_to_end_ns >= observation.service_ns for observation in observations))
        summary = benchmark.summarize(observations, 0.2, 20)
        self.assertEqual(0, summary["errors"])
        self.assertGreater(summary["latency_ns"]["p99"], 0)
        self.assertEqual(4, summary["operations"]["search"]["successes"])

    def test_write_fraction_uses_exact_rational_schedule(self):
        ratio = benchmark.Fraction("0.01")
        selected = [
            sequence
            for sequence in range(750)
            if (sequence * ratio.numerator) % ratio.denominator < ratio.numerator
        ]
        self.assertEqual(list(range(0, 750, 100)), selected)

    def test_client_sends_raw_ndjson_without_json_quoting(self):
        received = []

        class RawHandler(Handler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                received.append(self.rfile.read(length))
                payload = b'{}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        server = ThreadingHTTPServer(("127.0.0.1", 0), RawHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = benchmark.Client(f"http://127.0.0.1:{server.server_port}", 2)
            observation = client.request(
                {"method": "POST", "path": "/ingest", "raw_body": '{"body":"alpha"}\n'},
                benchmark.time.perf_counter_ns(),
                "write",
            )
            client.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
        self.assertIsNone(observation.error)
        self.assertEqual([b'{"body":"alpha"}\n'], received)


def lsm_manifest(active_paths, obsolete_paths, active_size_bytes):
    raw = bytearray(b"ALSMMAN1")
    raw.extend(struct.pack("<IQII", 8, 4, len(active_paths), len(obsolete_paths)))
    for index, path in enumerate(active_paths, 1):
        encoded = str(path).encode()
        smallest = b"a"
        largest = b"z"
        raw.extend(struct.pack("<QIQ", index, 0, active_size_bytes))
        raw.extend(struct.pack("<QQQQQ", 0, 0, 0, 0, 0))
        raw.extend(struct.pack("<IIIIII", len(encoded), 0, len(smallest), 0, len(largest), 1))
        raw.extend(encoded + smallest + largest)
    for path in obsolete_paths:
        encoded = str(path).encode()
        raw.extend(struct.pack("<QI", 0, len(encoded)))
        raw.extend(encoded)
    return bytes(raw)


if __name__ == "__main__":
    unittest.main()
