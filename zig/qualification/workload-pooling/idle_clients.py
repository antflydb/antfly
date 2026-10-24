#!/usr/bin/env python3
"""Measure an existing native server using thresholds fixed in a manifest.

Uses only the standard library. The startup manifest retains unauthenticated
sockets. Authenticated manifests create an isolated fixture and verify held
cursor rows across COMMIT without changing server policy.
"""

import argparse
import os
import http.client
import json
import pathlib
import platform
import socket
import select
import subprocess
import time
import urllib.parse
from pgwire_peer import Peer


def sample(pid):
    values = subprocess.check_output(
        ["ps", "-o", "rss=,vsz=", "-p", str(pid)], text=True
    ).split()
    if len(values) != 2:
        raise RuntimeError("server process disappeared")
    if platform.system() == "Darwin":
        threads = (
            len(
                subprocess.check_output(
                    ["ps", "-M", "-p", str(pid)], text=True
                ).splitlines()
            )
            - 1
        )
        descriptors = subprocess.check_output(
            ["/usr/sbin/lsof", "-nP", "-a", "-p", str(pid), "-Ff"], text=True
        ).splitlines()
        fds = sum(line[1:].isdigit() for line in descriptors if line.startswith("f"))
    elif platform.system() == "Linux":
        task = pathlib.Path("/proc") / str(pid)
        threads = len(list((task / "task").iterdir()))
        fds = len(list((task / "fd").iterdir()))
    else:
        raise RuntimeError("process sampling requires macOS or Linux")
    if platform.system() == "Linux":
        fields = (task / "stat").read_text().rsplit(")", 1)[1].split()
        cpu_seconds = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    else:
        clock = subprocess.check_output(
            ["ps", "-o", "cputime=", "-p", str(pid)], text=True
        ).strip()
        cpu_seconds = 0.0
        for component in clock.split(":"):
            cpu_seconds = cpu_seconds * 60 + float(component)
    return {
        "cpu_seconds": cpu_seconds,
        "rss_bytes": int(values[0]) * 1024,
        "virtual_bytes": int(values[1]) * 1024,
        "threads": threads,
        "fds": fds,
    }


def auth_headers():
    value = os.environ.get("ANTFLY_QUALIFICATION_AUTHORIZATION")
    return {"Authorization": value} if value else {}


def status(url):
    target = urllib.parse.urlsplit(url)
    cls = (
        http.client.HTTPSConnection
        if target.scheme == "https"
        else http.client.HTTPConnection
    )
    conn = cls(target.hostname, target.port, timeout=2)
    started = time.monotonic()
    try:
        conn.request("GET", target.path or "/status", headers=auth_headers())
        response = conn.getresponse()
        response.read()
        if response.status != 200:
            raise RuntimeError(f"status returned {response.status}")
        return time.monotonic() - started
    finally:
        conn.close()


def seed_fixture(target, rows, payload_bytes=0):
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=30)
    base = (target.path or "/db/v1/status").rsplit("/", 1)[0]

    def request(path, value):
        connection.request(
            "POST",
            base + path,
            json.dumps(value),
            headers=dict(auth_headers(), **{"Content-Type": "application/json"}),
        )
        response = connection.getresponse()
        body = response.read()
        if response.status not in (200, 201):
            raise RuntimeError(
                f"qualification fixture returned {response.status}: {body.decode()}"
            )

    try:
        request(
            "/tables/pooling_qualification",
            {
                "num_shards": 1,
                "schema": {
                    "storage_mode": "relational",
                    "default_type": "row",
                    "document_schemas": {
                        "row": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "integer"},
                                    "payload": {"type": "string"},
                                },
                                "additionalProperties": False,
                            }
                        }
                    },
                },
            },
        )
        for offset in range(0, rows, 64):
            request(
                "/tables/pooling_qualification/batch",
                {
                    "inserts": {
                        f"row-{i}": {
                            "id": 9007199254740993 + i,
                            "payload": "x" * payload_bytes,
                        }
                        for i in range(offset, min(rows, offset + 64))
                    },
                    "sync_level": "write",
                },
            )
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--status-url", required=True)
    parser.add_argument("--pgwire-host", default="127.0.0.1")
    parser.add_argument("--pgwire-port", required=True, type=int)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument(
        "--memory-map",
        action="store_true",
        help="retain native memory category diagnostics",
    )
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    thresholds = manifest["thresholds"]
    target = urllib.parse.urlsplit(args.status_url)
    http_peers, sql_peers = [], []
    receipt = {"manifest": manifest, "platform": platform.platform(), "pid": args.pid}
    failures = []
    authenticated = manifest.get("pgwire_mode", "startup") == "authenticated"
    try:
        if authenticated:
            seed_fixture(target, manifest["fixture_rows"])
            with_peer = Peer(args.pgwire_host, args.pgwire_port)
            try:
                with_peer.query("SELECT id FROM pooling_qualification ORDER BY id")
            finally:
                with_peer.close()
        for _ in range(3):
            status(args.status_url)
        receipt["baseline"] = sample(args.pid)
    except Exception as error:
        receipt.update(passed=False, failures=[str(error)], setup_failed=True)
        args.output.write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps({"passed": False, "failures": receipt["failures"]}))
        return True
    try:
        for _ in range(manifest["http_connections"]):
            peer = http.client.HTTPConnection(target.hostname, target.port, timeout=2)
            http_peers.append(peer)
            peer.request("GET", target.path or "/status", headers=auth_headers())
            response = peer.getresponse()
            response.read()
            if response.status != 200 or peer.sock is None:
                raise RuntimeError("HTTP idle peer was not retained")
        for index in range(manifest["pgwire_connections"]):
            if authenticated:
                peer = Peer(args.pgwire_host, args.pgwire_port)
                sql_peers.append(peer)
                if index < manifest["held_cursors"]:
                    peer.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY", b"T")
                    peer.query(
                        "DECLARE held CURSOR WITH HOLD FOR SELECT id FROM pooling_qualification ORDER BY id",
                        b"T",
                    )
                    first = peer.query("FETCH NEXT FROM held", b"T")
                    if first != [["9007199254740993"]]:
                        raise RuntimeError("held cursor lost exact integer row")
                    peer.query("COMMIT")
                elif index < manifest["held_cursors"] + manifest.get(
                    "extended_portals", 0
                ):
                    peer.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY", b"T")
                    if peer.open_portal(
                        "SELECT id FROM pooling_qualification ORDER BY id"
                    ) != [["9007199254740993"]]:
                        raise RuntimeError("extended portal lost exact integer")
            else:
                sql_peers.append(
                    socket.create_connection(
                        (args.pgwire_host, args.pgwire_port), timeout=2
                    )
                )
        time.sleep(manifest["sample_interval_seconds"])
        # TCP connect can succeed even if the listener immediately sheds it.
        closed = select.select(
            [peer.sock if authenticated else peer for peer in sql_peers], [], [], 0
        )[0]
        receipt["retained_pgwire_clients"] = len(sql_peers) - len(closed)
        if closed:
            failures.append(f"{len(closed)} pgwire clients were shed")
        receipt["retained_http_clients"] = sum(
            peer.sock is not None for peer in http_peers
        )
        receipt["idle_samples"] = []
        receipt["control_latencies_seconds"] = []
        for _ in range(manifest["samples"]):
            receipt["idle_samples"].append(sample(args.pid))
            receipt["control_latencies_seconds"].append(status(args.status_url))
            time.sleep(manifest["sample_interval_seconds"])
        if authenticated:
            expected = [
                [str(9007199254740993 + i)] for i in range(1, manifest["fixture_rows"])
            ]
            for peer in sql_peers[: manifest["held_cursors"]]:
                if peer.query("FETCH ALL FROM held") != expected:
                    raise RuntimeError("held cursor continuation changed after COMMIT")
                peer.query("CLOSE held")
            portal_start = manifest["held_cursors"]
            portal_end = portal_start + manifest.get("extended_portals", 0)
            for peer in sql_peers[portal_start:portal_end]:
                if peer.execute_portal(0) != expected:
                    raise RuntimeError("extended portal continuation changed")
                peer.close_portal()
                peer.query("ROLLBACK")
            receipt["verified_extended_portals"] = portal_end - portal_start
            for peer in sql_peers[portal_end:]:
                if (
                    peer.query("SELECT id FROM pooling_qualification ORDER BY id")
                    != [["9007199254740993"]] + expected
                ):
                    raise RuntimeError("authenticated session query failed")
            receipt["verified_held_cursors"] = manifest["held_cursors"]
            receipt["verified_authenticated_sessions"] = len(sql_peers)
        if args.memory_map:
            if platform.system() == "Darwin":
                receipt["memory_map_summary"] = subprocess.check_output(
                    ["/usr/bin/vmmap", "-summary", str(args.pid)], text=True
                )
            elif platform.system() == "Linux":
                receipt["memory_map_summary"] = (
                    pathlib.Path("/proc") / str(args.pid) / "smaps_rollup"
                ).read_text()
    except Exception as error:
        failures.append(str(error))
    finally:
        receipt["retained_http_peers_before_close"] = sum(
            peer.sock is not None for peer in http_peers
        )
        receipt["constructed_pgwire_peers_before_close"] = len(sql_peers)
        for peer in http_peers:
            peer.close()
        for peer in sql_peers:
            peer.close()
        time.sleep(manifest["recovery_seconds"])
        receipt["recovery"] = sample(args.pid)
    baseline = receipt["baseline"]
    receipt["idle_peak"] = (
        {key: max(s[key] for s in receipt["idle_samples"]) for key in baseline}
        if receipt.get("idle_samples")
        else baseline
    )
    peak, recovery = receipt["idle_peak"], receipt["recovery"]
    checks = {
        "idle_rss_growth_bytes": peak["rss_bytes"] - baseline["rss_bytes"],
        "idle_thread_growth": peak["threads"] - baseline["threads"],
        "idle_fd_growth": peak["fds"] - baseline["fds"],
        "control_latency_seconds": max(receipt["control_latencies_seconds"])
        if receipt.get("control_latencies_seconds")
        else None,
        "recovery_fd_growth": recovery["fds"] - baseline["fds"],
        "recovery_rss_growth_bytes": recovery["rss_bytes"] - baseline["rss_bytes"],
    }
    for key, value in checks.items():
        if value is None or value > thresholds[key]:
            failures.append(f"{key}: {value} exceeds {thresholds[key]}")
    receipt.update(checks=checks, failures=failures, passed=not failures)
    args.output.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"passed": not failures, "checks": checks, "failures": failures}))
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
