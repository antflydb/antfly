#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Bounded PostgreSQL oracle for exact source-owned read and document campaigns.

Run with uv run --no-project --with 'psycopg[binary]==3.3.6'
python scripts/generate_sql_postgres_reference.py read (or document).
Requires PostgreSQL 18+ binaries, selected by ANTFLY_PG_BIN or PATH.
Every run owns a temporary server, listening only on its private Unix socket.
Original statement SQL and $n parameters are passed unchanged to RawCursor.
Discovery never changes the implementation ledger. No SQLite fallback exists.
"""

import argparse
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from datetime import date, datetime, time
from decimal import Decimal
import getpass
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
from tempfile import TemporaryDirectory

from generate_sql_document_reference import SEEDS
from generate_sql_parity_read_reference import EMPTY_CONTRACTS, parameter

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "zig/pkg/antfly-embedded/src/sql/fixtures"
ROW_LIMIT = 4096

# Independent ordering observers for source cases with non-unique sort keys.
# The actual source statement still executes unchanged. These bounded queries
# expose the entire eligible peer frontier, so LIMIT cannot make PostgreSQL's
# arbitrary tie selection into a false requirement for the native engine.
ORDER_OBSERVERS = {
    "sql-0233": "SELECT id, ceil(least(amount,quantity,100)) AS order_key FROM usage_records WHERE floor(round(abs(amount-quantity))) > $1 ORDER BY order_key",
    "sql-0238": "SELECT greatest(amount,quantity,0) AS max_amount, least(amount,quantity,100) AS min_amount, least(amount,quantity,100) AS order_key FROM usage_records WHERE greatest(amount,quantity,0) > $1 ORDER BY order_key",
    "sql-0239": "SELECT id, octet_length(status) AS status_bytes, character_length(status) AS order_key FROM usage_records WHERE char_length(status) > $1 ORDER BY order_key DESC",
    "sql-0240": "SELECT id, bit_length(status) AS status_bits, bit_length(status) AS order_key FROM usage_records WHERE bit_length(status) > $1 ORDER BY order_key DESC",
    "sql-0302": "SELECT id, created_at AS order_key FROM usage_records WHERE status = ANY(ARRAY['closed','pending']::text[]) OR status = 'open' AND amount > 20 ORDER BY order_key DESC",
    "sql-1226": "SELECT organization_id, COUNT(*) FILTER (WHERE lower(status) LIKE ANY(ARRAY['op%', 'ready%'])) AS openish_count, COUNT(*) FILTER (WHERE lower(status) LIKE ANY(ARRAY['op%', 'ready%'])) AS order_key FROM usage_records GROUP BY organization_id ORDER BY order_key DESC",
    "sql-1227": "SELECT organization_id, COUNT(*) FILTER (WHERE lower(status) LIKE SOME(ARRAY['op%', 'ready%'])) AS openish_count, COUNT(*) FILTER (WHERE lower(status) LIKE SOME(ARRAY['op%', 'ready%'])) AS order_key FROM usage_records GROUP BY organization_id ORDER BY order_key DESC",
}


@contextmanager
def postgres():
    import psycopg

    configured = os.environ.get("ANTFLY_PG_BIN")
    binary = Path(configured) if configured else None
    if binary is None:
        located = shutil.which("initdb")
        if located:
            binary = Path(located).parent
        elif Path("/opt/homebrew/opt/postgresql@18/bin/initdb").exists():
            binary = Path("/opt/homebrew/opt/postgresql@18/bin")
        else:
            raise RuntimeError(
                "PostgreSQL 18+ is required; set ANTFLY_PG_BIN (no SQLite fallback)"
            )
    version = subprocess.check_output([binary / "postgres", "--version"], text=True)
    match = re.search(r"PostgreSQL\) (\d+)", version)
    if not match or int(match[1]) < 18:
        raise RuntimeError("PostgreSQL 18+ is required for this campaign")
    with TemporaryDirectory(prefix="antfly-sql-pg-", dir="/tmp") as directory:
        data = Path(directory) / "data"
        subprocess.run(
            [
                binary / "initdb",
                "-D",
                data,
                "--no-locale",
                "--encoding=UTF8",
                "--auth=trust",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )
        started = False
        try:
            subprocess.run(
                [
                    binary / "pg_ctl",
                    "-D",
                    data,
                    "-l",
                    Path(directory) / "server.log",
                    "-o",
                    f"-k {directory} -c listen_addresses=''",
                    "-w",
                    "start",
                ],
                check=True,
                capture_output=True,
                timeout=30,
            )
            started = True
            with psycopg.connect(
                host=directory,
                port=5432,
                user=getpass.getuser(),
                dbname="postgres",
                autocommit=True,
            ) as db:
                # Never execute fixtures against a foreign database, even if
                # inherited libpq service settings redirect a connection.
                actual_data = db.execute("SHOW data_directory").fetchone()[0]
                if Path(actual_data).resolve() != data.resolve():
                    raise RuntimeError(
                        "oracle connection is not the owned temporary server"
                    )
                # Limit reads, recursive execution and accidental lock waits.
                db.execute("SET statement_timeout = '2s'")
                db.execute("SET lock_timeout = '250ms'")
                db.execute("SET work_mem = '4MB'")
                db.execute("SET temp_file_limit = '16MB'")
                db.execute("SET timezone = 'UTC'")
                yield db
        finally:
            if started or (data / "postmaster.pid").exists():
                subprocess.run(
                    [binary / "pg_ctl", "-D", data, "-m", "immediate", "-w", "stop"],
                    check=True,
                    capture_output=True,
                    timeout=30,
                )


def properties(schema):
    return schema["document_schemas"][schema["default_type"]]["schema"]["properties"]


def pg_type(prop):
    kind = prop.get("type")
    return {
        "integer": "bigint",
        "numeric": "double precision",
        "number": "double precision",
        "boolean": "boolean",
        "text": "text",
        "keyword": "text",
        "string": "text",
        "datetime": "timestamptz",
        "json": "jsonb",
        "object": "jsonb",
        "array": "jsonb",
    }[kind]


def encoded(value):
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite numeric needs a dedicated wire contract")
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("non-finite floating point needs a dedicated wire contract")
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, list):
        return [encoded(cell) for cell in value]
    if isinstance(value, dict):
        return {name: encoded(cell) for name, cell in value.items()}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ValueError(f"{type(value).__name__} needs a dedicated typed wire contract")


def parameters(case):
    from psycopg.types.json import Jsonb

    result = []
    for cell in case["params"]:
        value = parameter(cell)
        if isinstance(cell, dict) and "json" in cell:
            value = Jsonb(json.loads(value) if isinstance(value, str) else value)
        result.append(value)
    return result


def create_table(db, name, props, rows, identity=False):
    from psycopg import sql
    from psycopg.types.json import Jsonb

    if (
        not props
        or len(props) > 128
        or any(not key.replace("_", "").isalnum() for key in props)
    ):
        raise ValueError("profile requires bounded ordinary column identifiers")
    columns = {"_id": {"type": "keyword"}, **props} if identity else props
    if identity and any(key.startswith("_") for key in props):
        raise ValueError("schema cannot replace reserved identity")
    definitions = [
        sql.SQL("{} {}{}").format(
            sql.Identifier(key),
            sql.SQL(pg_type(prop)),
            sql.SQL(" PRIMARY KEY" if identity and key == "_id" else ""),
        )
        for key, prop in columns.items()
    ]
    db.execute(
        sql.SQL("CREATE TABLE public.{} ({})").format(
            sql.Identifier(name), sql.SQL(",").join(definitions)
        )
    )
    insert = sql.SQL("INSERT INTO public.{} VALUES ({})").format(
        sql.Identifier(name), sql.SQL(",").join(sql.Placeholder() for _ in columns)
    )
    # Physical seed order must not serve as an implicit SQL ORDER BY contract.
    for row in sorted(rows, key=lambda row: row["key"]):
        values = []
        for key, prop in columns.items():
            value = row["key"] if identity and key == "_id" else row["value"].get(key)
            if pg_type(prop) == "jsonb" and value is not None:
                value = Jsonb(value)
            values.append(value)
        db.execute(insert, values)


def execute(db, case, read=False):
    import psycopg

    if re.search(
        r"\b(CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|random|randomblob)\b",
        case["sql"],
        re.IGNORECASE,
    ):
        raise ValueError("nondeterministic/native clock profile required")
    # A client-side cursor buffers the entire PG result before fetchmany().
    # Use a server-side raw cursor for reads so the row bound is real, without
    # appending LIMIT or changing the source's positional SQL parameters.
    transaction = db.transaction(force_rollback=True) if read else nullcontext()
    with transaction:
        if read:
            db.execute("SET TRANSACTION READ ONLY")
        cursor = (
            psycopg.RawServerCursor(db, "antfly_reference")
            if read
            else psycopg.RawCursor(db)
        )
        with cursor:
            return cursor_result(cursor, case, read)


def cursor_result(cursor, case, read):
    cursor.execute(case["sql"], parameters(case))
    # DECLARE ... FOR accepts SELECT, not arbitrary mutation statements.
    tag = "SELECT" if read else cursor.statusmessage.split()[0]
    if (read and tag != "SELECT") or (
        not read and tag not in {"INSERT", "UPDATE", "DELETE"}
    ):
        raise ValueError("statement does not match the campaign execution contract")
    rows = cursor.fetchmany(ROW_LIMIT + 1) if cursor.description else []
    if len(rows) > ROW_LIMIT:
        raise ValueError("reference exceeds row budget")
    if not rows and read and case["id"] not in EMPTY_CONTRACTS:
        raise ValueError("empty result does not exercise this shape")
    if not read and cursor.rowcount <= 0:
        raise ValueError("non-exercising mutation")
    return {
        "id": case["id"],
        "columns": [column.name for column in cursor.description]
        if cursor.description
        else [],
        "rows": [[encoded(cell) for cell in row] for row in rows],
        # JSON null and SQL NULL decode to Python None; preserve the wire
        # provenance from libpq rather than guessing from decoded values.
        "sql_nulls": [
            [cursor.pgresult.get_value(i, j) is None for j in range(len(row))]
            for i, row in enumerate(rows)
        ],
        "column_oids": [column.type_code for column in cursor.description]
        if cursor.description
        else [],
        "affected": 0 if read else cursor.rowcount,
    }


def mutation_reference(
    db, cases, profile, row_limit=ROW_LIMIT, byte_limit=16 * 1024 * 1024
):
    """Exact mutations, isolated by savepoints, with complete multi-table state.

    Psycopg 3.3.6 streaming discards the terminal command result. The pinned
    adapter below retains its authentic command tag/count instead of inferring
    affected rows from RETURNING or buffering the complete libpq result.
    This is an oracle-only adapter, not production execution machinery.
    """
    import psycopg
    from contextlib import closing
    from psycopg import sql
    from psycopg.generators import fetch
    from psycopg.pq import ExecStatus

    if psycopg.__version__ != "3.3.6":
        raise RuntimeError("mutation streaming oracle requires pinned psycopg 3.3.6")
    if not 0 < row_limit <= ROW_LIMIT:
        raise ValueError("invalid mutation reference row limit")
    if not 0 < byte_limit <= 64 * 1024 * 1024:
        raise ValueError("invalid mutation reference byte limit")

    class StreamingMutationCursor(psycopg.RawCursor):
        terminal = None

        def _stream_fetchone_gen(self, first):
            result = yield from fetch(self._pgconn)
            if result is None:
                return None
            if result.status in {ExecStatus.SINGLE_TUPLE, ExecStatus.TUPLES_CHUNK}:
                self.pgresult = result
                self._tx.set_pgresult(result, set_loaders=first)
                if first:
                    self._make_row = self._make_row_maker()
                return result
            if result.status in {ExecStatus.TUPLES_OK, ExecStatus.COMMAND_OK}:
                self.terminal = result
                while (yield from fetch(self._pgconn)) is not None:
                    raise ValueError("multiple SQL results are not a mutation contract")
                return None
            return self._raise_for_result(result)

    def collect(cursor, query, params, budget, label):
        rows, nulls = [], []
        # Single-row libpq mode bounds outstanding rows; byte admission bounds
        # retained decoded output across RETURNING and every post-state table.
        # Closing a partially consumed generator cancels/drains before rollback.
        with closing(cursor.stream(query, params)) as stream:
            for row in stream:
                if len(rows) >= row_limit:
                    raise ValueError(label + " exceeds row budget")
                raw = [cursor.pgresult.get_value(0, i) for i in range(len(row))]
                size = sum(len(cell) for cell in raw if cell is not None)
                if size > budget[0]:
                    raise ValueError(label + " exceeds byte budget")
                budget[0] -= size
                rows.append([encoded(cell) for cell in row])
                nulls.append([cell is None for cell in raw])
        terminal = cursor.terminal
        if terminal is None or not terminal.command_status:
            raise ValueError("missing command completion")
        return {
            "command_tag": terminal.command_status.decode("ascii").split()[0],
            "affected": terminal.command_tuples,
            "columns": [
                terminal.fname(i).decode("utf-8") for i in range(terminal.nfields)
            ],
            "column_oids": [terminal.ftype(i) for i in range(terminal.nfields)],
            "rows": rows,
            "sql_nulls": nulls,
        }

    tables = [
        {
            "name": "usage_records",
            "schema": profile["schema"],
            "rows": profile["rows"],
            "primary_key": profile.get("primary_key", []),
            "unique": profile.get("unique", []),
            "checks": profile.get("checks", []),
            "foreign_keys": profile.get("foreign_keys", []),
            "indexes": profile.get("indexes", []),
        },
        *profile.get("additional_tables", []),
    ]
    names = [table["name"] for table in tables]
    if len(tables) > 8 or len(set(names)) != len(names):
        raise ValueError("mutation profile requires distinct bounded table names")
    if sum(len(table["rows"]) for table in tables) > row_limit:
        raise ValueError("mutation seed rows exceed the profile budget")
    entries, excluded = [], []
    with db.transaction(force_rollback=True):
        for table in tables:
            props = properties(table["schema"])
            if any(prop.get("type") == "array" for prop in props.values()):
                raise ValueError("typed SQL arrays require a dedicated column profile")
            if any(table.get(field) for field in ("checks", "foreign_keys", "indexes")):
                raise ValueError("constraint/index owner profile is not declared")
            if len(table.get("unique", [])) > 128 or any(
                not key for key in table.get("unique", [])
            ):
                raise ValueError("unique constraints require bounded nonempty keys")
            # Identity/default producers need per-case sequence/state reset;
            # savepoint rollback alone is not an oracle for those contracts.
            if any("generated" in prop or "default" in prop for prop in props.values()):
                raise ValueError(
                    "generated/default owner profile requires explicit reset"
                )
            create_table(db, table["name"], props, table["rows"])
            keys = [("PRIMARY KEY", table.get("primary_key", []))]
            keys += [("UNIQUE", key) for key in table.get("unique", [])]
            for kind, columns in keys:
                if not columns:
                    continue
                if len(set(columns)) != len(columns) or set(columns) - set(props):
                    raise ValueError(
                        "constraint columns must belong to the pinned schema"
                    )
                db.execute(
                    sql.SQL("ALTER TABLE public.{} ADD {} ({})").format(
                        sql.Identifier(table["name"]),
                        sql.SQL(kind),
                        sql.SQL(",").join(map(sql.Identifier, columns)),
                    )
                )
        for case in cases:
            try:
                if re.search(
                    r"\b(CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|random|randomblob|gen_random_uuid)\b",
                    case["sql"],
                    re.I,
                ):
                    raise ValueError(
                        "nondeterministic/native producer profile required"
                    )
                with db.transaction(force_rollback=True):
                    remaining = [byte_limit]
                    with StreamingMutationCursor(db) as cursor:
                        entry = collect(
                            cursor,
                            case["sql"],
                            parameters(case),
                            remaining,
                            "mutation RETURNING",
                        )
                    tag = entry["command_tag"]
                    affected = entry["affected"]
                    if tag not in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
                        raise ValueError("statement is not a mutation")
                    if affected is None or not 0 < affected <= row_limit:
                        raise ValueError(
                            "mutation is non-exercising or exceeds row budget"
                        )
                    entry.update(id=case["id"], final_tables={})
                    for table in tables:
                        columns = list(properties(table["schema"]))
                        query = sql.SQL("SELECT {} FROM public.{} ORDER BY {}").format(
                            sql.SQL(",").join(map(sql.Identifier, columns)),
                            sql.Identifier(table["name"]),
                            sql.SQL(",").join(map(sql.Identifier, columns)),
                        )
                        with StreamingMutationCursor(db) as cursor:
                            state = collect(
                                cursor, query, None, remaining, "mutation final state"
                            )
                        if state.pop("command_tag") != "SELECT":
                            raise ValueError("invalid final state command")
                        state.pop("affected")
                        entry["final_tables"][table["name"]] = state
                    entries.append(entry)
            except (psycopg.Error, ValueError, KeyError, TypeError) as error:
                excluded.append(
                    {
                        "id": case["id"],
                        "sqlstate": getattr(error, "sqlstate", None),
                        "reason": str(error),
                    }
                )
    return {
        "format": 3,
        "reference": "PostgreSQL exact SQL",
        "profile": profile,
        "entries": entries,
        "excluded": excluded,
    }


def read_reference(db, cases, profile):
    import psycopg

    entries, excluded = [], []
    with db.transaction(force_rollback=True):
        create_table(
            db, "usage_records", properties(profile["schema"]), profile["rows"]
        )
        for table in profile.get("additional_tables", []):
            create_table(db, table["name"], properties(table["schema"]), table["rows"])
        for case in cases:
            try:
                with db.transaction(force_rollback=True):
                    db.execute("SET TRANSACTION READ ONLY")
                    entry = execute(db, case, read=True)
                    observer_sql = ORDER_OBSERVERS.get(case["id"])
                    if "sql-1345" <= case["id"] <= "sql-1365":
                        # These originals sort by their projected amount. The
                        # observer exposes the full peer frontier, not a second
                        # arbitrary LIMIT selection. Source execution above is
                        # unchanged; OFFSET cases have unique fixture keys.
                        if case["id"] in {"sql-1358", "sql-1359", "sql-1365"}:
                            observer_sql, count = re.subn(r" LIMIT 5$", "", case["sql"])
                            if count != 1:
                                raise ValueError("lateral observer shape changed")
                            observer_sql = observer_sql.replace(
                                "latest.amount AS latest_amount FROM",
                                "latest.amount AS latest_amount, latest.amount AS order_key FROM",
                                1,
                            )
                    if observer_sql:
                        observer = execute(db, {**case, "sql": observer_sql}, read=True)
                        groups = []
                        key = object()
                        for row, nulls in zip(
                            observer["rows"], observer["sql_nulls"], strict=True
                        ):
                            if row[-1] != key:
                                key = row[-1]
                                groups.append({"rows": [], "sql_nulls": []})
                            groups[-1]["rows"].append(row[:-1])
                            groups[-1]["sql_nulls"].append(nulls[:-1])
                        entry["ordered_groups"] = groups
                        validate_ordered_groups(entry)
                    entries.append(entry)
            except (psycopg.Error, ValueError, KeyError, TypeError) as error:
                excluded.append({"id": case["id"], "reason": str(error)})
    return {
        "format": 2,
        "reference": "PostgreSQL exact SQL",
        "profile": profile,
        "entries": entries,
        "excluded": excluded,
    }


def validate_ordered_groups(entry):
    """Check a complete ordered prefix, permitting only genuine peer ties."""
    offset = 0
    for group in entry["ordered_groups"]:
        count = min(len(group["rows"]), len(entry["rows"]) - offset)
        candidates = list(zip(group["rows"], group["sql_nulls"], strict=True))
        for row, nulls in zip(
            entry["rows"][offset : offset + count],
            entry["sql_nulls"][offset : offset + count],
            strict=True,
        ):
            candidate = (row, nulls)
            if candidate not in candidates:
                raise ValueError("result is not an eligible ordered peer prefix")
            candidates.remove(candidate)
        offset += count
        if offset == len(entry["rows"]):
            return
    raise ValueError("ordered reference frontier is incomplete")


def document_reference(db, cases, schemas):
    import psycopg
    from psycopg import sql

    entries, excluded = [], []
    for case in cases:
        schema = schemas[case["id"]]
        raw = json.dumps(schema)
        if '"generated"' in raw or "x-antfly-column-name" in raw:
            excluded.append(
                {
                    "id": case["id"],
                    "reason": "native generated/alias owner profile required",
                }
            )
            continue
        try:
            with db.transaction(force_rollback=True):
                props = properties(schema)
                create_table(db, "docs", props, SEEDS, identity=True)
                # SQL NULL and a missing document property are different
                # physical states. PostgreSQL UPDATE OF triggers expose the
                # actual assignment columns without parsing/rewriting the
                # source SQL or guessing from unchanged post-image values.
                db.execute(
                    "CREATE TEMP TABLE field_updates (identity text, field text)"
                )
                db.execute("""CREATE FUNCTION pg_temp.record_field_update() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN
                    INSERT INTO pg_temp.field_updates VALUES (NEW._id, TG_ARGV[0]);
                    RETURN NEW; END $$""")
                for index, name in enumerate(props):
                    db.execute(
                        sql.SQL(
                            "CREATE TRIGGER {} AFTER UPDATE OF {} ON public.docs FOR EACH ROW EXECUTE FUNCTION pg_temp.record_field_update({})"
                        ).format(
                            sql.Identifier(f"field_{index}"),
                            sql.Identifier(name),
                            sql.Literal(name),
                        )
                    )
                entry = execute(db, case)
                assignments = set(
                    db.execute(
                        "SELECT identity, field FROM pg_temp.field_updates"
                    ).fetchall()
                )
                final = db.execute("SELECT * FROM docs ORDER BY _id").fetchmany(
                    ROW_LIMIT + 1
                )
                if len(final) > ROW_LIMIT or set(row[0] for row in final) - {
                    seed["key"] for seed in SEEDS
                }:
                    raise ValueError("insert identity/default profile required")
                stored = {row[0]: row[1:] for row in final}
                expected = []
                for seed in SEEDS:
                    if seed["key"] not in stored:
                        continue
                    value = deepcopy(seed["value"])
                    for name, cell in zip(props, stored[seed["key"]], strict=True):
                        cell = encoded(cell)
                        original = seed["value"].get(name)
                        if (
                            type(original) is int
                            and isinstance(cell, float)
                            and cell.is_integer()
                            and int(cell) == original
                        ):
                            cell = original
                        if (
                            name in seed["value"]
                            or cell is not None
                            or (seed["key"], name) in assignments
                        ):
                            value[name] = cell
                    expected.append({"key": seed["key"], "value": value})
                native_schema = deepcopy(schema)
                metadata = properties(native_schema).get("metadata")
                if metadata is not None and metadata.get("type") == "json":
                    # The source uses a historical relational-only shorthand
                    # for an object-valued document property. Publish the
                    # current document JSON Schema shape explicitly, retain
                    # the source schema separately and grant no index proof.
                    metadata["type"] = "object"
                    metadata["additionalProperties"] = True
                entry.update(schema=schema, native_schema=native_schema, final=expected)
                entries.append(entry)
        except (psycopg.Error, ValueError, KeyError, TypeError) as error:
            excluded.append({"id": case["id"], "reason": str(error)})
    return {
        "format": 2,
        "reference": "PostgreSQL exact SQL",
        "profile": "guarded-document-field-mutations",
        "seeds": SEEDS,
        "entries": entries,
        "excluded": excluded,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", choices=["read", "document", "lateral", "mutation"])
    parser.add_argument(
        "--check",
        type=Path,
        help="verify only golden IDs; never silently exclude a failing case",
    )
    args = parser.parse_args()
    manifest = json.loads((FIXTURES / f"sql_{args.campaign}_campaign.json").read_text())
    requested = [entry["id"] for entry in manifest["entries"]]
    expected = json.loads(args.check.read_text()) if args.check else None
    if expected:
        requested = [entry["id"] for entry in expected["entries"]]
        if expected.get("reference") != "PostgreSQL exact SQL":
            parser.error("golden must declare the PostgreSQL oracle")
    known = {entry["id"] for entry in manifest["entries"]}
    if (
        not requested
        or len(requested) != len(set(requested))
        or not set(requested) <= known
    ):
        parser.error("golden requires unique nonempty campaign IDs")
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
        "entries"
    ]
    cases = [case for case in inventory if case["id"] in set(requested)]
    with postgres() as db:
        if args.campaign in {"read", "lateral", "mutation"}:
            profile = json.loads(
                (FIXTURES / f"sql_{args.campaign}_campaign_profile.json").read_text()
            )
            result = (
                mutation_reference if args.campaign == "mutation" else read_reference
            )(db, cases, profile)
        else:
            result = document_reference(
                db,
                cases,
                {entry["id"]: entry["schema"] for entry in manifest["entries"]},
            )
        result["server_version"] = db.info.server_version
    if expected:
        if result["excluded"]:
            parser.error(f"PostgreSQL rejected golden IDs: {result['excluded']}")
        for output in (result, expected):
            output.pop("excluded", None)
            output.pop("server_version", None)
            for entry in output["entries"]:
                if "ordered_groups" in entry:
                    validate_ordered_groups(entry)
                    entry["row_count"] = len(entry.pop("rows"))
                    entry.pop("sql_nulls")
        if result != expected:
            parser.error("PostgreSQL reference drift")
        print(
            f"Verified {len(result['entries'])} exact PostgreSQL {args.campaign} contracts"
        )
    else:
        print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
