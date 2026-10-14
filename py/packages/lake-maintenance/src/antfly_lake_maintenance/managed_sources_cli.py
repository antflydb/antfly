# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Managed adapter operator entry point; secrets are resolved only at runtime."""

import argparse
import json
import os
import signal
import sys
import threading

from .managed_sources import (
    AntflyTarget,
    GCSNotifications,
    ManagedSources,
    PostgreSQLCDC,
    S3Notifications,
    exclusive_s3_writer,
)
from .server import resolve
from .store import Store


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument(
        "action", choices=("define", "reconcile", "poll", "status", "remove", "run")
    )
    parser.add_argument("--interval", type=float, default=10)
    arguments = parser.parse_args()
    with open(arguments.config) as stream:
        config = json.load(stream)
    runtime = resolve(config.get("runtime", {}))
    providers = {}
    kind = config["sources"][arguments.source]["kind"]
    if kind == "s3":
        import boto3

        session = boto3.Session(**runtime.get("aws_session", {}))
        s3, sqs, sts = (
            session.client(name, **runtime.get(name, {}))
            for name in ("s3", "sqs", "sts")
        )
        providers[kind] = S3Notifications(
            s3,
            sqs,
            verify_exclusive_configuration_writer=exclusive_s3_writer(
                s3, sts, config["notification_writer_arn"]
            ),
        )
    elif kind == "gcs":
        from google.cloud import pubsub_v1, storage

        providers[kind] = GCSNotifications(
            storage.Client(), pubsub_v1.PublisherClient(), pubsub_v1.SubscriberClient()
        )
    elif kind == "postgres":
        import psycopg

        def connect(reference):
            if not reference.startswith("${secret:") or not reference.endswith("}"):
                raise ValueError("invalid PostgreSQL secret reference")
            key = reference[9:-1]
            return psycopg.connect(os.environ[key], connect_timeout=10)

        providers[kind] = PostgreSQLCDC(connect)
    else:
        raise ValueError("unknown managed adapter")
    manager = ManagedSources(
        Store(runtime.get("io_properties", {})),
        config["authority_uri"],
        AntflyTarget(config["antfly_endpoint"], runtime.get("antfly_headers", {})),
        providers,
    )
    if arguments.action == "run":
        if not 1 <= arguments.interval <= 3600:
            parser.error("interval must be between one second and one hour")
        stopped = threading.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stopped.set())
        failures = 0
        while not stopped.is_set():
            try:
                result = manager.step(arguments.source)
                print(json.dumps(result), flush=True)
                failures = 0
                if result["phase"] == "removed":
                    return
            except Exception as error:
                failures = min(failures + 1, 6)
                print(
                    json.dumps(
                        {"source": arguments.source, "error_type": type(error).__name__}
                    ),
                    file=sys.stderr,
                    flush=True,
                )
            stopped.wait(min(300, arguments.interval * (2**failures)))
        return
    if arguments.action == "define":
        result = manager.define(arguments.source, config["sources"][arguments.source])
    else:
        result = getattr(manager, arguments.action)(arguments.source)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
