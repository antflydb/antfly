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

"""PostgreSQL domain lifecycle witnesses; native qualification is separate."""

import argparse
import json
from pathlib import Path

from generate_sql_operator_reference import reference

CATALOG = """
SELECT t.typname,t.typtype::text,t.typnotnull,
       pg_get_expr(t.typdefaultbin,0) AS default_expression,
       obj_description(t.oid,'pg_type') AS comment,
       (SELECT string_agg(c.conname || ':' || c.convalidated::text || ':' ||
                           pg_get_constraintdef(c.oid), ';' ORDER BY c.conname)
        FROM pg_constraint c WHERE c.contypid=t.oid) AS constraints
FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
WHERE n.nspname='public' AND t.typname IN
 ('positive_amount','status_text','usage_status','usage_status_enum')
ORDER BY t.typname
"""
ROWS = "SELECT id,amount::numeric,status::text FROM domain_rows ORDER BY id"
BEFORE = [
    "CREATE DOMAIN null_provenance AS text NOT NULL",
    "CREATE TABLE null_provenance_rows(d null_provenance)",
    "INSERT INTO null_provenance_rows SELECT (SELECT d FROM null_provenance_rows WHERE false)",
    "SELECT d IS NULL AS admitted_typed_null FROM null_provenance_rows",
    "DROP TABLE null_provenance_rows",
    "DROP DOMAIN null_provenance",
    "CREATE DOMAIN check_provenance AS text",
    "CREATE TABLE check_provenance_rows(d check_provenance)",
    "INSERT INTO check_provenance_rows VALUES ('old')",
    "ALTER DOMAIN check_provenance ADD CONSTRAINT current_state CHECK (VALUE='good') NOT VALID",
    "INSERT INTO check_provenance_rows VALUES ('old')",
    "INSERT INTO check_provenance_rows SELECT d FROM check_provenance_rows",
    "SELECT d FROM check_provenance_rows ORDER BY d",
    "DROP TABLE check_provenance_rows",
    "DROP DOMAIN check_provenance",
    "CREATE DOMAIN naming_probe AS text CHECK (VALUE IS NOT NULL) CONSTRAINT naming_probe_check CHECK (VALUE = 'queued')",
    "CREATE DOMAIN naming_probe AS text CONSTRAINT naming_probe_check CHECK (VALUE = 'queued') CHECK (VALUE IS NOT NULL) CHECK (VALUE <> 'bad')",
    "SELECT conname FROM pg_constraint WHERE contypid='naming_probe'::regtype ORDER BY oid",
    "DROP DOMAIN naming_probe",
    "CREATE DOMAIN order_probe AS int CONSTRAINT z CHECK(VALUE>0) CONSTRAINT a CHECK(VALUE>0)",
    "SELECT (-1)::order_probe",
    "DROP DOMAIN order_probe",
    "sql-0306",
    CATALOG,
    "sql-0307",
    CATALOG,
    "CREATE DOMAIN usage_status AS text CHECK (VALUE IN ('queued','done'))",
    "CREATE TYPE usage_status_enum AS ENUM ('queued','done')",
    "sql-0149",
    CATALOG,
    "sql-0150",
    CATALOG,
    "CREATE TABLE domain_rows(id BIGINT PRIMARY KEY,amount positive_amount,status status_text)",
    "INSERT INTO domain_rows(id,amount) VALUES(1,10),(2,200),(3,NULL)",
    ROWS,
    "INSERT INTO domain_rows(id,amount) VALUES(4,-1)",
    ROWS,
    "INSERT INTO domain_rows VALUES(4,1,NULL)",
    ROWS,
    "ALTER DOMAIN positive_amount SET NOT NULL",
    CATALOG,
    ROWS,
    "DELETE FROM domain_rows WHERE id=3",
    "sql-0308",
    CATALOG,
    ROWS,
    "SELECT NULL::positive_amount",
    "SELECT r.id,r.amount IS NULL AS absent FROM (VALUES(99::bigint)) x(id) LEFT JOIN domain_rows r ON r.id=x.id",
    "ALTER DOMAIN status_text SET DEFAULT 'done'",
    CATALOG,
    ROWS,
    "INSERT INTO domain_rows(id,amount) VALUES(4,5)",
    ROWS,
    "ALTER DOMAIN positive_amount ADD CONSTRAINT bounded CHECK (VALUE < 100) NOT VALID",
    CATALOG,
    "INSERT INTO domain_rows(id,amount) VALUES(5,300)",
    ROWS,
    "ALTER DOMAIN positive_amount VALIDATE CONSTRAINT bounded",
    CATALOG,
    "UPDATE domain_rows SET amount=20 WHERE id=2",
    "ALTER DOMAIN positive_amount VALIDATE CONSTRAINT bounded",
    CATALOG,
    ROWS,
    "DROP DOMAIN positive_amount",
    CATALOG,
    ROWS,
]
COLD = [
    CATALOG,
    ROWS,
    "INSERT INTO domain_rows(id,amount) VALUES(5,NULL)",
    "INSERT INTO domain_rows(id,amount) VALUES(5,100)",
    ROWS,
    "SELECT ('done'::status_text)::text AS value,pg_typeof('done'::status_text)::text AS domain",
]
AFTER = [
    "sql-0310",
    CATALOG,
    "SELECT id,status::text FROM domain_rows ORDER BY id",
    "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='domain_rows' ORDER BY ordinal_position",
    "CREATE DOMAIN positive_amount AS numeric CHECK (VALUE > 0)",
    "sql-0309",
    CATALOG,
    "DROP DOMAIN IF EXISTS positive_amount CASCADE",
    "DROP TABLE domain_rows",
    "DROP DOMAIN status_text",
    "DROP DOMAIN usage_status",
    "DROP TYPE usage_status_enum",
    CATALOG,
]
IDS = (
    "sql-0149",
    "sql-0150",
    "sql-0306",
    "sql-0307",
    "sql-0308",
    "sql-0309",
    "sql-0310",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    result = reference(
        BEFORE,
        COLD,
        AFTER,
        IDS,
        "PostgreSQL 18 named domains, validation, defaults, comments and dependent retirement; native activation remains unqualified",
    )
    originals = [
        step for phase in ("before", "after") for step in result[phase] if "id" in step
    ]
    assert {step["id"] for step in originals} == set(IDS)
    assert all("sqlstate" not in step for step in originals), originals
    failures = {
        step["sql"]: step["sqlstate"]
        for phase in ("before", "cold")
        for step in result[phase]
        if "sqlstate" in step
    }
    assert failures["ALTER DOMAIN positive_amount SET NOT NULL"] == "23502"
    assert (
        failures["ALTER DOMAIN positive_amount VALIDATE CONSTRAINT bounded"] == "23514"
    )
    assert failures["DROP DOMAIN positive_amount"] == "2BP01"
    assert failures["SELECT NULL::positive_amount"] == "23502"
    encoded = json.dumps(result, indent=2) + "\n"
    if args.check:
        assert args.output.read_text() == encoded, "PostgreSQL domain witness drift"
    else:
        args.output.write_text(encoded)


if __name__ == "__main__":
    main()
