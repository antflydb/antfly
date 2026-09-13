"""Internal OHR raw-PDF ingest comparison; no external parsing in the timed path."""

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path
from urllib.parse import quote

ROOT = None
CIRCUS = None
ARCHIVE_SHA256 = "f9bc65f383172c4ea47940c47dfab01dd36c03a120bc0450d7a962917098c783"


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def completed_log_offset(path):
    """Checkpoint the last complete record at a fixed, append-only log EOF.

    A logger may flush one line in several writes. Never wait for it or include
    a partial record; it remains available to a later checkpoint/validation.
    Scan backwards in bounded blocks rather than loading the entire server log.
    """
    with path.open("rb") as stream:
        end = stream.seek(0, os.SEEK_END)
        while end:
            start = max(0, end - 8192)
            stream.seek(start)
            block = stream.read(end - start)
            if len(block) != end - start:
                raise ValueError("server log was truncated during checkpoint")
            newline = block.rfind(b"\n")
            if newline >= 0:
                return start + newline + 1
            end = start
    return 0


def prepare():
    archive_digest = sha256(ROOT / "pdfs.zip")
    if archive_digest != ARCHIVE_SHA256:
        raise ValueError(f"Unexpected OHR archive SHA-256: {archive_digest}")
    corpus = ROOT / "corpus"
    rows = []
    with zipfile.ZipFile(ROOT / "pdfs.zip") as archive:
        for relative, role in CURATED_FIXTURES:
            names = [n for n in archive.namelist() if n.endswith(relative)]
            if len(names) != 1:
                raise ValueError((relative, names))
            target = corpus / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(names[0]))
            pages = len(PdfReader(target).pages)
            rows.append(
                {
                    "path": relative,
                    "role": role,
                    "pages": pages,
                    "bytes": target.stat().st_size,
                    "sha256": sha256(target),
                }
            )
    save(ROOT / "corpus.json", {"archive_sha256": archive_digest, "documents": rows})
    print(json.dumps(rows, indent=2), flush=True)


def config(mode, ocr_model, embed_model, consumers=1):
    from consumers import table_with_consumers

    indexes = api.hierarchy_indexes()
    # The Circus adapter predates the typed graph artifact source API.
    graph = indexes["document_units"]
    graph["source"].pop("kind", None)
    graph["artifact"]["source"] = {
        "type": "field",
        "value": graph["artifact"].pop("field"),
    }
    producer = graph["artifact"]["producer_json"]
    producer["config"]["ocr"]["mode"] = mode
    producer["config"]["ocr"]["config"]["model"] = ocr_model
    indexes["document_vectors"]["embedder"]["model"] = embed_model
    for enrichment in indexes["document_text"]["enrichments"]:
        if enrichment["kind"] == "asset":
            enrichment["producer_json"] = json.dumps(producer)
    return table_with_consumers({"num_shards": 1, "indexes": indexes}, consumers)


def wait_until(fn, seconds, proc=None):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        if proc and proc.poll() is not None:
            raise RuntimeError(f"Antfly exited: {proc.returncode}")
        try:
            last = fn()
            if last:
                return last
        except (OSError, RuntimeError) as exc:
            last = str(exc)
        time.sleep(0.25)
    raise TimeoutError(f"Condition not ready in {seconds}s: {last}")


def coverage_ready(vector_status, documents):
    coverage = vector_status.get("coverage", {})
    return (
        all(
            coverage.get(k) is True
            for k in ("complete", "healthy", "observation_complete")
        )
        and all(
            coverage.get(k) == documents
            for k in ("source_total", "produced", "covered")
        )
        and coverage.get("terminal_failed", 0) == 0
        and vector_status.get("searchable_vectors", 0) >= documents
    )


def artifact_errors(selected, manifests):
    errors = []
    for row in selected:
        manifest = manifests[row["path"]]
        if manifest.get("unit_count") != row["pages"]:
            errors.append(
                f"{row['path']}: page coverage {manifest.get('unit_count')}/{row['pages']}"
            )
        if not manifest.get("chunk_count"):
            errors.append(f"{row['path']}: no chunks")
        if manifest.get("ocr_failed_count", 0):
            errors.append(f"{row['path']}: OCR failures")
        if row["role"] == "ocr_required" and not manifest.get("ocr_selected_count"):
            errors.append(f"{row['path']}: required OCR was not selected")
    return errors


def unit_text_hashes(manifests, fetch_unit, geometry=None):
    """Verify equivalent retained page text outside the measured interval."""
    hashes = {}
    for source, manifest in sorted(manifests.items()):
        state = json.loads(manifest["state_json"])
        keys = state["unit_keys"]
        if len(keys) != manifest["unit_count"] or len(set(keys)) != len(keys):
            raise ValueError(f"{source}: incomplete or duplicate unit keys")
        units = {}
        if geometry is not None:
            geometry[source] = {}
        for key in keys:
            unit = fetch_unit(key)
            identity = unit["unit_id"]
            text = unit["text"]
            if identity in units or not isinstance(text, str):
                raise ValueError(f"{source}: invalid unit text or identity")
            units[identity] = hashlib.sha256(text.encode("utf-8")).hexdigest()
            if geometry is not None:
                provenance = unit.get("provenance") or {}
                page = {
                    field: unit.get(field, provenance.get(field))
                    for field in (
                        "page_number",
                        "page_bbox",
                        "page_rotation",
                        "ocr_render_dpi",
                        "ocr_effective_render_dpi",
                        "ocr_rendered_width",
                        "ocr_rendered_height",
                    )
                }
                warning = unit.get("extraction_warning") or provenance.get(
                    "extraction_warning"
                )
                page["render_quality_warnings"] = [
                    part
                    for part in (warning or "").split(";")
                    if part.startswith("pdf_render_quality:")
                ]
                # Main's image-only PDF extraction leaves rotation unknown;
                # retain/compare that null, but do not invent a zero rotation.
                required = ["page_number", "page_bbox"]
                if unit.get("ocr_attempted", provenance.get("ocr_attempted")):
                    required.extend(
                        [
                            "ocr_render_dpi",
                            "ocr_effective_render_dpi",
                            "ocr_rendered_width",
                            "ocr_rendered_height",
                        ]
                    )
                geometry[source][identity] = page
                missing = [field for field in required if page[field] is None]
                if missing:
                    raise ValueError(
                        f"{source}/{identity}: missing page geometry: {missing}"
                    )
        hashes[source] = units
    return hashes


RENDER_CONTROLS = {
    "render_workers": "ANTFLY_ENRICHMENT_OCR_RENDER_PARALLEL_PAGES",
    "render_prefetch": "ANTFLY_ENRICHMENT_PDF_RENDER_PREFETCH_BATCHES",
    "render_memory_bytes": "ANTFLY_ENRICHMENT_OCR_RENDER_INFLIGHT_BYTES",
}


def runtime_environment(args, ambient):
    environment = {k: v for k, v in ambient.items() if not k.startswith("ANTFLY_")}
    if args.read_profile:
        environment["ANTFLY_INFERENCE_READ_PROFILE"] = "1"
    if args.reader_batch_size is not None:
        environment["ANTFLY_INFERENCE_READ_BATCH_SIZE"] = str(args.reader_batch_size)
    for field, variable in RENDER_CONTROLS.items():
        value = getattr(args, field, None)
        if value is not None:
            environment[variable] = str(value)
    return environment


def run(args):
    out = ROOT / args.name
    out.mkdir()  # Never reuse a previous database or overwrite a run.
    try:
        return run_created(args, out)
    except Exception as exc:
        if not (out / "failure.json").exists():
            save(out / "failure.json", {"error": repr(exc), "completed_trials": 0})
        raise


def run_created(args, out):
    models_dir = (args.models_dir or ROOT / "models").resolve(strict=True)
    model_files = {}
    for model in sorted(models_dir.glob("*/*")):
        for path in sorted(model.rglob("*")):
            if path.is_file():
                if path.name == ".antfly-download-in-progress":
                    raise ValueError(f"Model download still in progress: {model}")
                model_files[str(path.relative_to(models_dir))] = {
                    "bytes": path.stat().st_size,
                    "sha256": sha256(path),
                }
    save(out / "models.json", model_files)
    selected = json.loads((ROOT / "corpus.json").read_text())["documents"]
    if args.suite == "small":
        roles = {"render_7_pages", "ocr_required", "born_digital", "textbook_qa"}
        selected = [r for r in selected if r["role"] in roles]
    elif args.suite == "scan":
        selected = [r for r in selected if r["role"] == "ocr_required"]
    elif args.suite == "text":
        roles = {"render_7_pages", "born_digital", "textbook_qa"}
        selected = [r for r in selected if r["role"] in roles]
    elif args.suite == "embedded":
        roles = {"render_7_pages", "born_digital"}
        selected = [r for r in selected if r["role"] in roles]
    elif args.suite == "throughput":
        # Fixed 51-page cohort, chosen by corpus roles, not run outcomes.
        roles = {"type1_type3_lifetime", "largest_pdf", "law_qa_url_encoding"}
        selected = [r for r in selected if r["role"] in roles]
    for row in selected:
        if sha256(ROOT / "corpus" / row["path"]) != row["sha256"]:
            raise ValueError(f"Corpus file changed: {row['path']}")
    origin = PdfOrigin(("127.0.0.1", 0), ROOT / "corpus", out / "origin.jsonl")
    thread = threading.Thread(target=origin.serve_forever, daemon=True)
    source_url = f"http://127.0.0.1:{origin.server_port}"
    server_config = json.loads(
        (
            CIRCUS / "benchmarks/OHR-Bench-harness/antfly-local-content-config.json"
        ).read_text()
    )
    save(out / "config.json", server_config)
    binary = Path(args.binary).resolve()
    command = [
        str(binary),
        "standalone",
        "--host",
        "127.0.0.1",
        "--port",
        str(args.port),
        "--health-port",
        str(args.port + 1),
        "--data-dir",
        str(out / "db"),
        "--models-dir",
        str(models_dir),
        "--config",
        str(out / "config.json"),
        "--process-memory-budget-mb",
        "16000",
    ]
    # Pin admission identically; keep model loading/table setup timing explicit.
    overrides = {k: v for k, v in os.environ.items() if k.startswith("ANTFLY_")}
    environment = runtime_environment(args, os.environ)
    provenance = {
        "binary": str(binary),
        "binary_sha256": sha256(binary),
        "command": command,
        "revision": args.revision,
        "mode": args.mode,
        "suite": args.suite,
        "batch": args.batch,
        "model_loading": "fresh process first trial; reused process later trials; table setup reported separately",
        "filesystem_cache": "uncontrolled/warm; no system cache flush",
        "removed_environment_keys": sorted(overrides),
        "read_profile": args.read_profile,
        "reader_batch_size": args.reader_batch_size,
        "consumers": args.consumers,
        "sync_level": args.sync_level,
        **{field: getattr(args, field, None) for field in RENDER_CONTROLS},
        "verify_unit_text": args.verify_unit_text,
        "platform": (
            os.uname()._asdict() if hasattr(os.uname(), "_asdict") else list(os.uname())
        ),
        "load_at_start": os.getloadavg(),
        "selected": selected,
        "python": sys.version,
        "circus_revision": subprocess.check_output(
            ["git", "-C", str(CIRCUS), "rev-parse", "HEAD"], text=True
        ).strip(),
    }
    save(out / "provenance.json", provenance)
    url = f"http://127.0.0.1:{args.port}"
    results = []
    log = (out / "antfly.log").open("w")
    process_started = time.perf_counter()
    proc = None
    try:
        proc = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT, env=environment
        )
        thread.start()
        print(f"Started {args.name}: pid={proc.pid}", flush=True)
        # Do not accidentally send table writes to an unrelated server using the
        # requested port. Require our child's successful bind before any HTTP.
        wait_until(
            lambda: (
                f"standalone public api listening on {url}"
                in (out / "antfly.log").read_text()
            ),
            120,
            proc,
        )
        wait_until(
            lambda: (
                api.json_request("GET", url + "/db/v1/tables", timeout=2) is not None
            ),
            120,
            proc,
        )
        startup_seconds = time.perf_counter() - process_started
        for trial in range(args.trials):
            # Checkpoint complete records without writing markers into the
            # server-owned log or changing the timed interval.
            profile_start = (
                completed_log_offset(out / "antfly.log") if args.read_profile else None
            )
            table_started = time.perf_counter()
            table = f"pdf_bench_{trial}"
            table_url = f"{url}/db/v1/tables/{table}"
            table_config = config(
                args.mode, args.ocr_model, args.embed_model, args.consumers
            )
            save(out / "table-config.json", table_config)
            api.json_request("POST", table_url, table_config)
            wait_until(
                lambda table_url=table_url: api.index_readiness(
                    api.json_request("GET", table_url + "/indexes")
                )[0],
                120,
                proc,
            )
            table_setup_seconds = time.perf_counter() - table_started
            records = {}
            for row in selected:
                key, record, count, error = api._source_record(
                    ROOT / "corpus" / row["path"], ROOT / "corpus", source_url
                )
                assert count == row["pages"] and not error
                records[key] = record
            access_start = (
                len((out / "origin.jsonl").read_text().splitlines())
                if (out / "origin.jsonl").exists()
                else 0
            )
            started = time.perf_counter()
            print(
                f"{args.name} trial={trial} ingest {len(records)} PDFs, {sum(r['pages'] for r in selected)} pages",
                flush=True,
            )
            groups = [records] if args.batch else [{k: v} for k, v in records.items()]
            responses = []
            for group in groups:
                response = api.json_request(
                    "POST",
                    table_url + "/batch",
                    {"inserts": group, "sync_level": args.sync_level},
                    timeout=args.timeout,
                )
                responses.append(response)
                save(out / f"responses-{trial}.json", responses)
                assert response.get("inserted") == len(group), response
            ack_seconds = time.perf_counter() - started

            document_count = len(records)

            def complete(
                table=table, table_url=table_url, trial=trial, count=document_count
            ):
                from consumers import all_consumers_complete, collect_manifests

                manifests = {
                    r["path"]: api._artifact_manifest(url, table, r["path"])
                    for r in selected
                }
                statuses = api.json_request("GET", table_url + "/indexes")
                save(
                    out / f"latest-{trial}.json",
                    {"manifests": manifests, "indexes": statuses},
                )
                if not all(
                    m.get("merge_status") == "converged" for m in manifests.values()
                ):
                    return None
                if not api.index_readiness(statuses)[0]:
                    return None
                vector = api._index_statuses(statuses)["document_vectors"]
                coverage = vector.get("coverage", {})
                if not (
                    coverage.get("observation_complete") is True
                    and coverage.get("complete") is True
                    and coverage.get("source_total") == count
                ):
                    return None
                consumer_manifests = {}
                if args.consumers > 1:
                    consumer_manifests = collect_manifests(
                        api, table_url, selected, args.consumers
                    )
                    if not all_consumers_complete(
                        api, statuses, consumer_manifests, count, args.consumers
                    ):
                        return None
                return {
                    "manifests": manifests,
                    "indexes": statuses,
                    "consumer_manifests": consumer_manifests,
                }

            finished = wait_until(complete, args.timeout, proc)
            elapsed = time.perf_counter() - started
            errors = artifact_errors(selected, finished["manifests"])
            text_hashes = None
            render_geometry = {} if args.verify_unit_text else None
            if args.verify_unit_text:
                retained_units = {}

                def fetch_unit(key, table_url=table_url, retained_units=retained_units):
                    unit = api.json_request(
                        "GET", table_url + "/documents/" + quote(key, safe="")
                    )
                    retained_units[key] = unit
                    return unit

                try:
                    text_hashes = unit_text_hashes(
                        finished["manifests"],
                        fetch_unit,
                        render_geometry,
                    )
                except (KeyError, TypeError, ValueError, OSError, RuntimeError) as exc:
                    errors.append(f"Unit text verification failed: {exc}")
                finally:
                    save(out / f"retained-units-{trial}.json", retained_units)
            if args.mode == "always" and any(
                finished["manifests"][r["path"]].get("ocr_attempted_count")
                != r["pages"]
                for r in selected
            ):
                errors.append("Forced OCR did not attempt every PDF page")
            vector = api._index_statuses(finished["indexes"])["document_vectors"]
            if not coverage_ready(vector, len(records)):
                errors.append(
                    "Vector coverage did not produce searchable vectors for every document"
                )
            consumer_results = []
            if args.consumers > 1:
                from consumers import consumer_name

                for consumer in range(1, args.consumers):
                    name = consumer_name("document_units_v1", consumer)
                    manifests = finished["consumer_manifests"][name]
                    errors.extend(
                        f"{name}: {error}"
                        for error in artifact_errors(selected, manifests)
                    )
                    if args.mode == "always" and any(
                        manifests[row["path"]].get("ocr_attempted_count")
                        != row["pages"]
                        for row in selected
                    ):
                        errors.append(f"{name}: forced OCR did not attempt every page")
                    vector = api._index_statuses(finished["indexes"])[
                        consumer_name("document_vectors", consumer)
                    ]
                    if not coverage_ready(vector, len(records)):
                        errors.append(f"{name}: incomplete vector coverage")
                    hashes, geometry = None, {}
                    if args.verify_unit_text:
                        try:
                            hashes = unit_text_hashes(manifests, fetch_unit, geometry)
                        except (
                            KeyError,
                            TypeError,
                            ValueError,
                            OSError,
                            RuntimeError,
                        ) as exc:
                            errors.append(
                                f"{name}: Unit text verification failed: {exc}"
                            )
                        finally:
                            save(out / f"retained-units-{trial}.json", retained_units)
                    consumer_results.append(
                        {
                            "name": name,
                            "unit_text_sha256": hashes,
                            "unit_render_geometry": geometry,
                            "searchable_vectors": vector.get("searchable_vectors"),
                            "manifest_counts": {
                                key: {
                                    field: value.get(field)
                                    for field in (
                                        "unit_count",
                                        "chunk_count",
                                        "ocr_attempted_count",
                                        "ocr_selected_count",
                                        "ocr_failed_count",
                                    )
                                }
                                for key, value in manifests.items()
                            },
                        }
                    )
            accesses = [
                json.loads(line)
                for line in (out / "origin.jsonl").read_text().splitlines()
            ]
            fetched = {
                r["path"]
                for r in accesses[access_start:]
                if r["method"] == "GET" and r["status"] == 200 and r["bytes"] > 0
            }
            errors.extend(
                f"{r['path']}: not fetched"
                for r in selected
                if r["path"] not in fetched
            )
            result = dict(
                trial=trial,
                profile_log={
                    "start_byte": profile_start,
                    "end_byte": completed_log_offset(out / "antfly.log"),
                }
                if args.read_profile
                else None,
                seconds=elapsed,
                acknowledgement_seconds=ack_seconds,
                startup_seconds=startup_seconds,
                table_setup_seconds=table_setup_seconds,
                setup_and_ingest_seconds=table_setup_seconds + elapsed,
                documents=len(records),
                pages=sum(r["pages"] for r in selected),
                passed=not errors,
                errors=errors,
                load=os.getloadavg(),
                unit_text_sha256=text_hashes,
                unit_render_geometry=render_geometry,
                consumer_results=consumer_results,
                **finished,
            )
            results.append(result)
            save(out / "results.json", results)
            print(
                json.dumps(
                    {
                        k: v
                        for k, v in result.items()
                        if k not in ("manifests", "indexes", "consumer_manifests")
                    }
                ),
                flush=True,
            )
            if errors:
                raise RuntimeError(errors)
        # Outside every timed interval: retain the resolved model contract for
        # diagnosing admission/batch-width decisions, not just file identities.
        try:
            capabilities = api.json_request("GET", url + "/ai/v1/models", timeout=5)
        except (OSError, RuntimeError, ValueError) as exc:
            capabilities = {"diagnostic_error": repr(exc)}
        save(out / "model-capabilities.json", capabilities)
    except Exception as exc:
        save(
            out / "failure.json", {"error": repr(exc), "completed_trials": len(results)}
        )
        raise
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        log.close()
        if thread.is_alive():
            origin.shutdown()
            thread.join(timeout=5)
        origin.server_close()


if __name__ == "__main__":

    def terminate(signum, _frame):
        # Let run_created's finally block stop its Antfly child and byte origin
        # when the paired driver is interrupted.
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "run"])
    parser.add_argument(
        "--work-dir",
        type=Path,
        required=True,
        help="Isolated directory containing pdfs.zip, models/, and run outputs",
    )
    parser.add_argument(
        "--circus-dir",
        type=Path,
        required=True,
        help="Read-only antfly-circus checkout with the OHR-Bench harness",
    )
    parser.add_argument("--models-dir", type=Path, help="Default: WORK_DIR/models")
    parser.add_argument("--binary")
    parser.add_argument("--revision")
    parser.add_argument("--name")
    parser.add_argument("--port", type=int, default=29680)
    parser.add_argument(
        "--suite",
        choices=["scan", "embedded", "text", "small", "throughput", "qualification"],
        default="small",
    )
    parser.add_argument("--ocr-model", default="antflydb/Florence-2-base:safetensors")
    parser.add_argument("--embed-model", default="BAAI/bge-small-en-v1.5:safetensors")
    parser.add_argument("--mode", choices=["auto", "always"], default="auto")
    parser.add_argument("--batch", action="store_true")
    parser.add_argument("--reader-batch-size", type=int, choices=[1, 2, 4, 8, 16])
    parser.add_argument("--consumers", type=int, choices=[1, 2], default=1)
    parser.add_argument(
        "--sync-level",
        choices=["full_index", "write"],
        default="full_index",
        help="Precommit enrichment or durable replay; both wait for full coverage in timing",
    )
    parser.add_argument("--render-workers", type=int, choices=[1, 2, 4, 8])
    parser.add_argument("--render-prefetch", type=int, choices=[0, 1])
    parser.add_argument("--render-memory-bytes", type=int)
    parser.add_argument(
        "--verify-unit-text",
        action="store_true",
        help="Hash retained page text after timing for exact A/B output checks",
    )
    parser.add_argument(
        "--read-profile",
        action="store_true",
        help="Enable reader diagnostics (not a timing run)",
    )
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()
    ROOT = args.work_dir.resolve()
    CIRCUS = args.circus_dir.resolve()
    sys.path.insert(0, str(CIRCUS / "benchmarks/OHR-Bench-harness"))
    from ohr import antfly as api
    from ohr.dataset import CURATED_FIXTURES, sha256
    from ohr.pdf_server import PdfOrigin
    from pypdf import PdfReader

    if args.action == "run" and not all((args.binary, args.revision, args.name)):
        parser.error("run requires --binary, --revision, and --name")
    if args.action == "run" and not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.name
    ):
        parser.error(
            "--name must be a single directory name starting with a letter or digit"
        )
    if args.trials < 1 or args.timeout <= 0:
        parser.error("--trials and --timeout must be positive")
    if args.render_memory_bytes is not None and args.render_memory_bytes <= 0:
        parser.error("--render-memory-bytes must be positive")
    prepare() if args.action == "prepare" else run(args)
