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

"""Run with uv run --no-project --with 'psycopg[binary]==3.3.6' python -m
unittest discover -s scripts -p test_generate_sql_postgres_reference.py.
These tests intentionally require PostgreSQL 18+: no skip or substitute oracle.
"""

from copy import deepcopy
import unittest
from unittest.mock import patch

from generate_sql_postgres_reference import (
    document_reference,
    execute,
    postgres,
    read_reference,
    mutation_reference,
    SEEDS,
    validate_ordered_groups,
)


class PostgresReferenceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = postgres()
        cls.db = cls.server.__enter__()
        cls.addClassCleanup(cls.server.__exit__, None, None, None)

    def test_original_catalog_subquery_defaults_are_not_postgres_features(self):
        import json
        from pathlib import Path

        fixtures = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
        )
        cases = json.loads((fixtures / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        selected = [case for case in cases if "sql-1109" <= case["id"] <= "sql-1140"]
        self.assertEqual(len(selected), 32)
        for case in selected:
            with self.subTest(id=case["id"]):
                with self.db.transaction(force_rollback=True):
                    if case["sql"].upper().startswith("ALTER"):
                        self.db.execute(
                            "CREATE TABLE usage_records(id uuid, status text, amount bigint)"
                        )
                    import psycopg

                    with self.assertRaises(psycopg.errors.FeatureNotSupported) as error:
                        with self.db.transaction(force_rollback=True):
                            self.db.execute(case["sql"])
                    self.assertEqual(error.exception.sqlstate, "0A000")

    def case(self, sql, params=()):
        return {"id": "sql-0001", "sql": sql, "params": params}

    def test_window_order_aliases_are_standalone_not_expression_variables(self):
        import psycopg

        rejected = [
            "SELECT row_number() OVER () AS n ORDER BY n+1",
            "SELECT row_number() OVER () AS n ORDER BY CAST(n AS bigint)",
            "SELECT row_number() OVER () AS n ORDER BY coalesce(n,0)",
            "SELECT row_number() OVER () AS n ORDER BY CASE WHEN TRUE THEN n ELSE 0 END",
            "SELECT row_number() OVER () ORDER BY row_number+1",
            'SELECT row_number() OVER () AS "n.total" ORDER BY "n.total"+1',
            "SELECT x,row_number() OVER (ORDER BY x) AS n FROM (SELECT 1 AS x) t ORDER BY n+1",
        ]
        for sql in rejected:
            with self.subTest(sql=sql):
                with self.db.transaction(force_rollback=True):
                    with self.assertRaises(psycopg.errors.UndefinedColumn) as error:
                        self.db.execute(sql)
                    self.assertEqual(error.exception.sqlstate, "42703")
        prefix = "SELECT -x AS x,row_number() OVER (ORDER BY x) AS n FROM (SELECT 2 AS x UNION ALL SELECT 1 UNION ALL SELECT 3) t ORDER BY "
        self.assertEqual(
            self.db.execute(prefix + "x DESC").fetchall(),
            [(-1, 1), (-2, 2), (-3, 3)],
        )
        self.assertEqual(
            self.db.execute(prefix + "(x+0) DESC").fetchall(),
            [(-3, 3), (-2, 2), (-1, 1)],
        )
        for sql in [
            "SELECT row_number() OVER (ORDER BY 1),row_number() OVER (ORDER BY 2) ORDER BY row_number",
            "SELECT row_number() OVER () AS n,rank() OVER () AS n ORDER BY n",
        ]:
            with self.subTest(sql=sql):
                with self.assertRaises(psycopg.errors.AmbiguousColumn) as error:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(sql)
                self.assertEqual("42702", error.exception.sqlstate)

    def test_original_window_output_expression_orders_are_not_postgres_features(self):
        import json
        from pathlib import Path
        import psycopg
        from generate_sql_postgres_reference import create_table, properties

        fixtures = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
        )
        profile = json.loads((fixtures / "sql_read_campaign_profile.json").read_text())
        inventory = json.loads((fixtures / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        cases = [case for case in inventory if case["id"] in {"sql-1219", "sql-1373"}]
        self.assertEqual(2, len(cases))
        self.assertNotIn("row_num", properties(profile["schema"]))
        with self.db.transaction(force_rollback=True):
            create_table(
                self.db, "usage_records", properties(profile["schema"]), profile["rows"]
            )
            for case in cases:
                with self.subTest(id=case["id"]):
                    with self.assertRaises(psycopg.errors.UndefinedColumn) as error:
                        with self.db.transaction(force_rollback=True):
                            self.db.execute(case["sql"])
                    self.assertEqual("42703", error.exception.sqlstate)
                    self.assertIn('"row_num"', str(error.exception))

    def mutation_profile(self):
        return {
            "schema": {
                "default_type": "row",
                "document_schemas": {
                    "row": {
                        "schema": {
                            "properties": {
                                "id": {"type": "keyword"},
                                "amount": {"type": "integer"},
                                "metadata": {"type": "json"},
                            }
                        }
                    }
                },
            },
            "primary_key": ["id"],
            "rows": [
                {
                    "key": "a",
                    "value": {"id": "u1", "amount": 5, "metadata": {"source": "api"}},
                },
                {"key": "b", "value": {"id": "u2", "amount": 9, "metadata": None}},
            ],
        }

    def test_mutation_stream_records_authentic_counts_nulls_and_fresh_state(self):
        cases = [
            self.case(
                "UPDATE usage_records SET amount=amount+$1 RETURNING id,amount", [1]
            ),
            self.case("DELETE FROM usage_records WHERE id='u1'"),
            self.case(
                "UPDATE usage_records SET metadata='null'::jsonb WHERE id='u1' RETURNING metadata"
            ),
            self.case(
                "UPDATE usage_records SET metadata=NULL WHERE id='u1' RETURNING metadata"
            ),
            self.case(
                "INSERT INTO usage_records (id,amount) VALUES ('u3',9007199254740993) RETURNING id,amount"
            ),
        ]
        result = mutation_reference(self.db, cases, self.mutation_profile())
        self.assertEqual([], result["excluded"])
        entries = result["entries"]
        self.assertEqual(
            ["UPDATE", "DELETE", "UPDATE", "UPDATE", "INSERT"],
            [entry["command_tag"] for entry in entries],
        )
        self.assertEqual([2, 1, 1, 1, 1], [entry["affected"] for entry in entries])
        self.assertEqual([["u1", 6], ["u2", 10]], entries[0]["rows"])
        self.assertEqual([25, 20], entries[0]["column_oids"])
        self.assertEqual([], entries[1]["rows"])
        self.assertEqual(
            [["u2", 9, None]], entries[1]["final_tables"]["usage_records"]["rows"]
        )
        self.assertEqual([[False]], entries[2]["sql_nulls"])
        self.assertEqual([[True]], entries[3]["sql_nulls"])
        self.assertEqual(
            [[False, False, False], [False, False, True]],
            entries[2]["final_tables"]["usage_records"]["sql_nulls"],
        )
        self.assertEqual([["u3", 9007199254740993]], entries[4]["rows"])
        self.assertEqual(3, len(entries[4]["final_tables"]["usage_records"]["rows"]))
        self.assertIsNone(
            self.db.execute("SELECT to_regclass('public.usage_records')").fetchone()[0]
        )

    def test_mutation_stream_joined_and_conflict_profiles_capture_every_table(self):
        profile = self.mutation_profile()
        profile["unique"] = [["amount"]]
        profile["additional_tables"] = [
            {
                "name": "archived_records",
                "schema": profile["schema"],
                "primary_key": ["id"],
                "rows": [
                    {
                        "key": "archived",
                        "value": {
                            "id": "u1",
                            "amount": 3,
                            "metadata": {"archive": True},
                        },
                    }
                ],
            }
        ]
        cases = [
            self.case(
                "UPDATE usage_records AS target SET amount=source.amount FROM archived_records AS source WHERE target.id=source.id RETURNING target.id,target.amount"
            ),
            self.case(
                "INSERT INTO usage_records (id,amount) VALUES ('u1',2) ON CONFLICT (id) DO UPDATE SET amount=usage_records.amount+excluded.amount RETURNING id,amount"
            ),
            self.case(
                "INSERT INTO usage_records (id,amount) VALUES ('u3',5) ON CONFLICT (amount) DO UPDATE SET metadata='null'::jsonb RETURNING id,metadata"
            ),
        ]
        result = mutation_reference(self.db, cases, profile)
        self.assertEqual([], result["excluded"])
        self.assertEqual([["u1", 3]], result["entries"][0]["rows"])
        self.assertEqual([["u1", 7]], result["entries"][1]["rows"])
        self.assertEqual([["u1", None]], result["entries"][2]["rows"])
        self.assertEqual([[False, False]], result["entries"][2]["sql_nulls"])
        for entry in result["entries"]:
            self.assertEqual(
                {"usage_records", "archived_records"}, set(entry["final_tables"])
            )
            self.assertEqual(
                [["u1", 3, {"archive": True}]],
                entry["final_tables"]["archived_records"]["rows"],
            )

    def test_mutation_stream_quota_and_constraint_failures_recover_the_connection(self):
        cases = [
            self.case("INSERT INTO usage_records (id,amount) VALUES ('u1',1)"),
            self.case(
                "INSERT INTO usage_records (id,amount) SELECT 'new_'||n, n FROM generate_series(1,20) n RETURNING id,amount"
            ),
            self.case(
                "INSERT INTO usage_records (id,amount) SELECT 'new_'||n, n FROM generate_series(1,20) n"
            ),
            self.case("SELECT 1"),
            self.case(
                "UPDATE usage_records SET amount=amount+1 WHERE id='u1' RETURNING amount"
            ),
        ]
        result = mutation_reference(
            self.db, cases, self.mutation_profile(), row_limit=2
        )
        self.assertEqual(4, len(result["excluded"]))
        self.assertEqual("23505", result["excluded"][0]["sqlstate"])
        self.assertIn("RETURNING exceeds row budget", result["excluded"][1]["reason"])
        self.assertIn("exceeds row budget", result["excluded"][2]["reason"])
        self.assertIn("not a mutation", result["excluded"][3]["reason"])
        self.assertEqual([[6]], result["entries"][0]["rows"])
        self.assertEqual(
            [["u1", 6, {"source": "api"}], ["u2", 9, None]],
            result["entries"][0]["final_tables"]["usage_records"]["rows"],
        )

    def test_original_postgres_mutation_campaign_goldens_are_complete_and_repeatable(
        self,
    ):
        import json
        from pathlib import Path

        fixtures = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
        )
        manifest = json.loads((fixtures / "sql_mutation_campaign.json").read_text())
        profile = json.loads(
            (fixtures / "sql_mutation_campaign_profile.json").read_text()
        )
        golden = json.loads(
            (fixtures / "sql_mutation_postgres_reference.json").read_text()
        )
        inventory = json.loads((fixtures / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        requested = {case["id"] for case in manifest["entries"]}
        self.assertEqual(235, len(requested))
        cases = [case for case in inventory if case["id"] in requested]
        result = mutation_reference(self.db, cases, profile)
        self.assertEqual(48, len(result["entries"]))
        self.assertEqual(187, len(result["excluded"]))
        self.assertEqual(
            requested, {case["id"] for case in result["entries"] + result["excluded"]}
        )
        self.assertEqual(profile, golden["profile"])
        self.assertEqual(golden["entries"], result["entries"])
        for entry in result["entries"]:
            self.assertGreater(entry["affected"], 0)
            self.assertEqual(
                {"usage_records", "archived_records", "source_records"},
                set(entry["final_tables"]),
            )
        # Classifications are profile-scoped discovery, never disposition credit.
        # In particular a missing arbiter is not a PostgreSQL syntax rejection.
        excluded = {case["id"]: case for case in result["excluded"]}
        self.assertEqual("42601", excluded["sql-0574"]["sqlstate"])
        self.assertEqual("42P10", excluded["sql-1394"]["sqlstate"])

    def test_mutation_stream_byte_quota_covers_returning_and_post_state(self):
        cases = [
            self.case(
                "UPDATE usage_records SET amount=amount+1 WHERE id='u1' RETURNING repeat('x',4096)"
            ),
            self.case(
                "UPDATE usage_records SET metadata=jsonb_build_object('payload',repeat('x',4096)) WHERE id='u1'"
            ),
            self.case(
                "UPDATE usage_records SET amount=amount+1 WHERE id='u1' RETURNING amount"
            ),
        ]
        result = mutation_reference(
            self.db, cases, self.mutation_profile(), byte_limit=128
        )
        self.assertEqual(2, len(result["excluded"]))
        self.assertIn("RETURNING exceeds byte budget", result["excluded"][0]["reason"])
        self.assertIn(
            "final state exceeds byte budget", result["excluded"][1]["reason"]
        )
        self.assertEqual([[6]], result["entries"][0]["rows"])
        self.assertEqual(
            [["u1", 6, {"source": "api"}], ["u2", 9, None]],
            result["entries"][0]["final_tables"]["usage_records"]["rows"],
        )

    def test_mutation_profile_gaps_fail_closed_before_case_execution(self):
        for change in [
            lambda profile: profile.update(primary_key=["missing"]),
            lambda profile: profile.update(unique=[[]]),
            lambda profile: profile.update(checks=["amount > 0"]),
            lambda profile: profile["schema"]["document_schemas"]["row"]["schema"][
                "properties"
            ]["metadata"].update(type="array"),
            lambda profile: profile["schema"]["document_schemas"]["row"]["schema"][
                "properties"
            ]["amount"].update(default=1),
            lambda profile: profile.update(
                additional_tables=[
                    {"name": "usage_records", "schema": profile["schema"], "rows": []}
                ]
            ),
        ]:
            profile = self.mutation_profile()
            change(profile)
            with self.assertRaises(ValueError):
                mutation_reference(
                    self.db, [self.case("DELETE FROM usage_records")], profile
                )
        self.assertIsNone(
            self.db.execute("SELECT to_regclass('public.usage_records')").fetchone()[0]
        )

    def test_like_explicit_escape_contracts(self):
        cases = [
            ("'bot_agent' LIKE 'bot!_%' ESCAPE '!'", True),
            ("'bot_agent' NOT LIKE 'bot!_%' ESCAPE '!'", False),
            ("'BOT_agent' ILIKE 'bot!_%' ESCAPE '!'", True),
            ("'a_b' LIKE 'aé_b' ESCAPE 'é'", True),
            ("'a%b' LIKE 'a%%b' ESCAPE '%'", True),
            ("'a_b' LIKE 'a__b' ESCAPE '_'", True),
            ("'a!xb' LIKE 'a!_b' ESCAPE ''", True),
            ("'a' LIKE 'a' ESCAPE NULL", None),
        ]
        for expression, expected in cases:
            with self.subTest(expression=expression):
                self.assertEqual(
                    self.db.execute("SELECT " + expression).fetchone()[0], expected
                )

    def profile(self):
        return {
            "schema": {
                "default_type": "row",
                "document_schemas": {
                    "row": {
                        "schema": {
                            "properties": {
                                "id": {"type": "integer"},
                                "metadata": {"type": "json"},
                            }
                        }
                    }
                },
            },
            "rows": [
                {
                    "key": "a",
                    "value": {"id": 9007199254740993, "metadata": {"source": "api"}},
                }
            ],
        }

    def test_postgres_version_and_private_listener(self):
        self.assertGreaterEqual(self.db.info.server_version, 180000)
        self.assertEqual("", self.db.execute("SHOW listen_addresses").fetchone()[0])

    def test_lateral_parent_scopes_materialization_and_recursion(self):
        cases = [
            (
                "SELECT l.n FROM (SELECT 1 AS n UNION ALL SELECT 2) p "
                "CROSS JOIN LATERAL (SELECT p.*) l ORDER BY l.n",
                [(1,), (2,)],
            ),
            (
                "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p "
                "CROSS JOIN LATERAL (WITH c AS MATERIALIZED (SELECT p.n AS x) "
                "SELECT a.x+b.x AS x FROM c a CROSS JOIN c b) l ORDER BY p.n",
                [(1, 2), (2, 4)],
            ),
            (
                "SELECT p.n,l.x FROM (SELECT 1 AS n UNION ALL SELECT 2) p "
                "CROSS JOIN LATERAL (WITH RECURSIVE r(n) AS (SELECT p.n "
                "UNION ALL SELECT n+1 FROM r WHERE n<p.n+1) "
                "SELECT sum(n) AS x FROM r) l ORDER BY p.n",
                [(1, 3), (2, 5)],
            ),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                self.assertEqual(expected, self.db.execute(sql).fetchall())

    def test_lateral_limit_offset_is_per_parent(self):
        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE edges(src bigint, dst bigint)")
            self.db.execute(
                "INSERT INTO edges SELECT n, n*10+k "
                "FROM generate_series(1,128) n CROSS JOIN generate_series(0,1) k"
            )
            rows = self.db.execute(
                "WITH RECURSIVE p(n) AS (SELECT 1 UNION ALL SELECT n+1 "
                "FROM p WHERE n<128) SELECT p.n,l.x FROM p LEFT JOIN LATERAL "
                "(SELECT e.dst AS x FROM edges e WHERE e.src=p.n "
                "ORDER BY e.dst DESC LIMIT 1 OFFSET 1) l ON true ORDER BY p.n"
            ).fetchall()
            self.assertEqual([(n, n * 10) for n in range(1, 129)], rows)

    def test_original_lateral_campaign_and_postgres_alias_scope(self):
        import json
        from pathlib import Path
        import psycopg
        from generate_sql_postgres_reference import create_table, properties

        fixtures = (
            Path(__file__).resolve().parents[1]
            / "zig/pkg/antfly-embedded/src/sql/fixtures"
        )
        profile = json.loads(
            (fixtures / "sql_lateral_campaign_profile.json").read_text()
        )
        self.assertEqual(1, len(profile["additional_tables"]))
        self.assertEqual("balance_records", profile["additional_tables"][0]["name"])
        self.assertEqual(profile["schema"], profile["additional_tables"][0]["schema"])
        inventory = json.loads((fixtures / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        cases = [
            case
            for case in inventory
            if "sql-1345" <= case["id"] <= "sql-1365"
            or case["id"] in {"sql-0549", "sql-1217", "sql-1218"}
        ]
        self.assertEqual(24, len(cases))
        result = read_reference(self.db, cases, profile)
        self.assertEqual(23, len(result["entries"]))
        self.assertEqual(["sql-1357"], [case["id"] for case in result["excluded"]])
        self.assertTrue(all(case["rows"] for case in result["entries"]))
        # LIMIT must not make an all-unmatched result a vacuous proof of the
        # inner predicate. Every two-column original exposes a real match.
        for case in result["entries"]:
            if len(case["columns"]) == 2:
                self.assertTrue(
                    any(not flags[1] for flags in case["sql_nulls"]), case["id"]
                )
        with self.db.transaction(force_rollback=True):
            create_table(
                self.db, "usage_records", properties(profile["schema"]), profile["rows"]
            )
            invalid = next(case for case in cases if case["id"] == "sql-1357")
            with self.assertRaises(psycopg.errors.UndefinedColumn) as error:
                with self.db.transaction(force_rollback=True):
                    self.db.execute(invalid["sql"])
            self.assertEqual("42703", error.exception.sqlstate)

    def test_logical_json_parameters_and_identity_casts_preserve_strings(self):
        from psycopg.types.json import Jsonb

        for value in ["pro", "null", "true", "12", "[1,2]", '{"x":1}', '"quoted"']:
            for expression in [
                "%s::jsonb",
                "CAST(%s::jsonb AS json)",
                "CAST(%s::jsonb AS jsonb)",
            ]:
                with self.subTest(value=value, expression=expression):
                    row = self.db.execute(
                        "SELECT " + expression, [Jsonb(value)]
                    ).fetchone()
                    self.assertEqual((value,), row)

    def test_jsonb_path_update_reference(self):
        import json
        from pathlib import Path
        import psycopg

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_json_path_update_reference.json"
            ).read_text()
        )
        self.assertEqual(32, len(fixture["entries"]))
        self.assertEqual(10, len(fixture["errors"]))
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                value, sql_null = self.db.execute(
                    "SELECT v,v IS NULL FROM (SELECT " + case["sql"] + " AS v) q"
                ).fetchone()
                self.assertEqual(case["value"], value)
                self.assertEqual(case.get("sql_null", False), sql_null)
        for case in fixture["errors"]:
            with self.subTest(sql=case["sql"]):
                with self.assertRaises(psycopg.Error) as error:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute("SELECT " + case["sql"])
                self.assertEqual(case["code"], error.exception.sqlstate)

    def test_jsonb_concatenation_reference(self):
        import json
        from pathlib import Path

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_json_concat_reference.json"
            ).read_text()
        )
        self.assertEqual(20, len(fixture["entries"]))
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                value, sql_null = self.db.execute(
                    "SELECT v,v IS NULL FROM (SELECT " + case["sql"] + " AS v) q"
                ).fetchone()
                self.assertEqual(case["value"], value)
                self.assertEqual(case.get("sql_null", False), sql_null)

    def test_returning_correlates_postimages_but_reads_the_statement_snapshot(self):
        cases = [
            (
                "UPDATE target SET n=n+10,cold='new' RETURNING n,(SELECT delta FROM source WHERE id='a') AS x",
                [(11, 10), (12, 10)],
            ),
            (
                "UPDATE target t SET n=n+9,cold='new' RETURNING n,(SELECT delta FROM source s WHERE s.delta=t.n) AS x",
                [(10, 10), (11, None)],
            ),
            (
                "DELETE FROM target RETURNING n,(SELECT delta FROM source WHERE id='a') AS x",
                [(1, 10), (2, 10)],
            ),
            (
                "UPDATE target SET n=n+10,cold='new' RETURNING n,(SELECT n FROM target WHERE _id='a') AS previous",
                [(11, 1), (12, 1)],
            ),
            (
                "UPDATE target SET cold='new' WHERE n<0 RETURNING (SELECT delta FROM source)",
                [],
            ),
            (
                "UPDATE target t SET n=n+10,cold=DEFAULT,g=DEFAULT RETURNING g,(SELECT delta FROM source s WHERE s.delta=t.g-2) AS matched",
                [(22, 20), (24, None)],
            ),
            (
                "INSERT INTO target(n,payload,cold) VALUES(9,'null'::jsonb,'new') RETURNING n,(SELECT delta FROM source WHERE id='a') AS x",
                [(9, 10)],
            ),
            (
                "INSERT INTO target(n,payload,cold) SELECT delta,'null'::jsonb,'new' FROM source WHERE id='a' RETURNING n,(SELECT delta FROM source WHERE id='b') AS x",
                [(10, 20)],
            ),
        ]
        for sql, expected in cases:
            with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                self.db.execute(
                    "CREATE TABLE target(_id text PRIMARY KEY DEFAULT 'fresh',n bigint,payload jsonb,cold text DEFAULT 'default',g bigint GENERATED ALWAYS AS (n*2) STORED)"
                )
                self.db.execute(
                    "INSERT INTO target(_id,n,payload,cold) VALUES ('a',1,'null','old'),('b',2,'null','old')"
                )
                self.db.execute("CREATE TABLE source(id text,delta bigint)")
                self.db.execute("INSERT INTO source VALUES ('a',10),('b',20)")
                self.assertEqual(expected, self.db.execute(sql).fetchall())

    def test_returning_cardinality_and_projection_errors_abort_the_mutation(self):
        import psycopg

        for sql, failure in [
            (
                "UPDATE target SET cold='new' RETURNING (SELECT delta FROM source)",
                psycopg.errors.CardinalityViolation,
            ),
            (
                "DELETE FROM target RETURNING (SELECT delta/0 FROM source WHERE id='a')",
                psycopg.errors.DivisionByZero,
            ),
        ]:
            with self.subTest(sql=sql), self.db.transaction(force_rollback=True):
                self.db.execute(
                    "CREATE TABLE target(_id text PRIMARY KEY,n bigint,payload jsonb,cold text)"
                )
                self.db.execute(
                    "INSERT INTO target VALUES ('a',1,'null','old'),('b',2,'null','old')"
                )
                self.db.execute("CREATE TABLE source(id text,delta bigint)")
                self.db.execute("INSERT INTO source VALUES ('a',10),('b',20)")
                with self.assertRaises(failure):
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(sql)
                self.assertEqual(
                    [("a", 1, "old"), ("b", 2, "old")],
                    self.db.execute(
                        "SELECT _id,n,cold FROM target ORDER BY _id"
                    ).fetchall(),
                )

    def test_json_and_typed_array_containment_reference(self):
        import json
        from pathlib import Path

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_containment_reference.json"
            ).read_text()
        )
        self.assertEqual(30, len(fixture["entries"]))
        for case in fixture["entries"]:
            with (
                self.subTest(sql=case["sql"]),
                self.db.transaction(force_rollback=True),
            ):
                self.assertEqual(
                    case["value"],
                    self.db.execute("SELECT " + case["sql"]).fetchone()[0],
                )

    def test_typed_array_quantifiers_against_exact_postgres_sql(self):
        import json
        from pathlib import Path

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_reference.json"
            ).read_text()
        )
        self.assertEqual(fixture["reference"], "PostgreSQL exact SQL")
        self.assertEqual(len(fixture["entries"]), 18)
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                with self.db.transaction(force_rollback=True):
                    self.db.execute("SET TRANSACTION READ ONLY")
                    cursor = self.db.execute("SELECT " + case["sql"])
                    self.assertEqual(cursor.fetchone(), (case["expected"],))
                    self.assertEqual(cursor.description[0].type_code, 16)

    def test_typed_array_shape_null_and_float_reference_boundaries(self):
        cases = {
            "array_ndims('{}'::int4[])": None,
            "cardinality('{}'::int4[])": 0,
            "array_length('{}'::int4[], 1)": None,
            "array_lower('[-1:0][0:1]={{1,NULL},{3,4}}'::int4[], 1)": -1,
            "array_upper('[-1:0][0:1]={{1,NULL},{3,4}}'::int4[], 1)": 0,
            "('[-1:0][0:1]={{1,NULL},{3,4}}'::int4[])[0][1]": 4,
            "ARRAY[NULL]::int4[] @> ARRAY[NULL]::int4[]": False,
            "ARRAY[NULL]::int4[] && ARRAY[NULL]::int4[]": False,
            "ARRAY[1]::int4[] @> ARRAY[1,1]::int4[]": True,
            "'[0:1]={1,2}'::int4[] < '[1:2]={1,2}'::int4[]": True,
            "'[0:1]={1,2}'::int4[] @> '[1:2]={1,2}'::int4[]": True,
            "ARRAY['NaN'::float8] = ARRAY['NaN'::float8]": True,
            "ARRAY['NaN'::float8] > ARRAY['Infinity'::float8]": True,
            "ARRAY[0.0::float8] = ARRAY[-0.0::float8]": True,
            "ARRAY['null'::jsonb] < ARRAY[NULL]::jsonb[]": True,
            "array_position('[-3:-1]={a,NULL,a}'::text[], 'a')": -3,
            "array_position('[-3:-1]={a,NULL,a}'::text[], NULL)": -2,
        }
        for expression, expected in cases.items():
            with self.subTest(sql=expression):
                with self.db.transaction(force_rollback=True):
                    self.db.execute("SET TRANSACTION READ ONLY")
                    actual = self.db.execute("SELECT " + expression).fetchone()
                    self.assertEqual(actual, (expected,))
        self.assertEqual(
            self.db.execute(
                "SELECT oid, typelem FROM pg_type WHERE oid = ANY(%s) ORDER BY oid",
                [[1000, 1005, 1007, 1009, 1016, 1021, 1022, 2951, 3807]],
            ).fetchall(),
            [
                (1000, 16),
                (1005, 21),
                (1007, 23),
                (1009, 25),
                (1016, 20),
                (1021, 700),
                (1022, 701),
                (2951, 2950),
                (3807, 3802),
            ],
        )

    def test_typed_array_binary_server_goldens_and_native_parameter_payloads(self):
        import json
        from pathlib import Path

        import psycopg
        from psycopg.adapt import Dumper
        from psycopg.pq import Format

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_binary_reference.json"
            ).read_text()
        )
        self.assertEqual(fixture["reference"], "PostgreSQL 18+ binary array_send")
        self.assertEqual(len(fixture["entries"]), 11)

        class Payload:
            def __init__(self, data):
                self.data = data

        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                dumper = type(
                    "TypedArrayDumper",
                    (Dumper,),
                    {
                        "oid": case["array_oid"],
                        "format": Format.BINARY,
                        "dump": lambda self, obj: obj.data,
                    },
                )
                self.db.adapters.register_dumper(Payload, dumper)
                with self.db.transaction(force_rollback=True):
                    self.db.execute("SET TRANSACTION READ ONLY")
                    with self.db.cursor(binary=True) as cursor:
                        cursor.execute("SELECT " + case["sql"])
                        self.assertEqual(cursor.pgresult.fformat(0), 1)
                        self.assertEqual(cursor.pgresult.ftype(0), case["array_oid"])
                        self.assertEqual(
                            cursor.pgresult.get_value(0, 0).hex(), case["binary"]
                        )
                    payload = Payload(
                        bytes.fromhex(case.get("native_binary", case["binary"]))
                    )
                    with psycopg.RawCursor(self.db) as cursor:
                        cursor.execute("SELECT $1 = (" + case["sql"] + ")", [payload])
                        self.assertEqual(cursor.fetchone(), (True,))
                        if case["element_type"] == "jsonb":
                            cursor.execute(
                                "SELECT e IS NULL, jsonb_typeof(e) FROM unnest($1) AS e",
                                [payload],
                            )
                            self.assertEqual(
                                cursor.fetchall(),
                                [
                                    (False, "null"),
                                    (True, None),
                                    (False, "object"),
                                    (False, "array"),
                                ],
                            )

    def test_prepared_parameter_descriptor_and_execution_contracts(self):
        import json
        from pathlib import Path

        import psycopg

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_parameter_frame_reference.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["entries"]), 24)
        self.assertEqual(len(fixture["errors"]), 9)
        supported = {
            "smallint",
            "integer",
            "bigint",
            "real",
            "double precision",
            "boolean",
            "text",
            "uuid",
            "jsonb",
            "timestamptz",
        }
        for case in fixture["entries"] + fixture["errors"]:
            with self.subTest(sql=case["sql"]):
                for name in case["types"]:
                    self.assertIn(name.removesuffix("[]"), supported)
                try:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(
                            "PREPARE frame_contract("
                            + ",".join(case["types"])
                            + ") AS SELECT "
                            + case["sql"]
                        )
                        if "oids" in case:
                            self.assertEqual(
                                self.db.execute(
                                    "SELECT parameter_types::oid[] FROM pg_prepared_statements "
                                    "WHERE name='frame_contract'"
                                ).fetchone()[0],
                                case["oids"],
                            )
                        arguments = psycopg.sql.SQL(",").join(
                            psycopg.sql.Literal(value) for value in case["values"]
                        )
                        actual = self.db.execute(
                            psycopg.sql.SQL("EXECUTE frame_contract({})").format(
                                arguments
                            )
                        ).fetchone()[0]
                        self.assertNotIn("code", case)
                        self.assertEqual(actual, case["value"])
                except psycopg.Error as error:
                    self.assertIn("code", case)
                    self.assertEqual(error.sqlstate, case["code"])
                finally:
                    self.db.execute("DEALLOCATE ALL")

    def test_prepared_parameter_inference_retains_builtin_widths(self):
        for expression, oids in (
            ("$1::smallint", [21]),
            ("$1 = ANY($2::smallint[])", [21, 1005]),
            (
                "ARRAY[cardinality($1::integer[]),cardinality($1::text[])]",
                [1007],
            ),
        ):
            with self.subTest(sql=expression):
                try:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(
                            "PREPARE inferred_frame AS SELECT " + expression
                        )
                        self.assertEqual(
                            self.db.execute(
                                "SELECT parameter_types::oid[] FROM pg_prepared_statements "
                                "WHERE name='inferred_frame'"
                            ).fetchone()[0],
                            oids,
                        )
                finally:
                    self.db.execute("DEALLOCATE ALL")

    def test_statement_parameter_frames_match_postgres_target_and_mutation_typing(self):
        import psycopg

        cases = (
            (
                "(bigint[])",
                "SELECT $1, cardinality($1::bigint[]) FROM frame_items "
                "WHERE needle = ANY($1) ORDER BY cardinality($1)",
                [1016],
            ),
            (
                "",
                "SELECT cardinality($1::bigint[]), $1 FROM frame_items "
                "WHERE needle = ANY($1) ORDER BY cardinality($1)",
                [1016],
            ),
            ("", "SELECT cardinality($1::integer[]), $1", [1007]),
            (
                "",
                "UPDATE frame_items SET needle = cardinality($1::integer[]) "
                "WHERE needle = $2",
                [1007, 20],
            ),
            ("", "UPDATE frame_items SET needle = $1 WHERE needle = $2", [20, 20]),
            (
                "",
                "INSERT INTO frame_items (needle) "
                "VALUES (cardinality($1::integer[])), ($2)",
                [1007, 20],
            ),
        )
        for declarations, sql, oids in cases:
            with self.subTest(sql=sql):
                try:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute("CREATE TABLE frame_items(needle bigint)")
                        self.db.execute(
                            "PREPARE statement_frame" + declarations + " AS " + sql
                        )
                        self.assertEqual(
                            self.db.execute(
                                "SELECT parameter_types::oid[] FROM pg_prepared_statements "
                                "WHERE name='statement_frame'"
                            ).fetchone()[0],
                            oids,
                        )
                finally:
                    self.db.execute("DEALLOCATE ALL")
        for sql in ("SELECT $1, cardinality($1::integer[])", "SELECT $1, $1::smallint"):
            with self.subTest(sql=sql):
                try:
                    with self.db.transaction(force_rollback=True):
                        with self.assertRaises(psycopg.Error) as error:
                            self.db.execute("PREPARE statement_frame AS " + sql)
                        self.assertEqual(error.exception.sqlstate, "42P08")
                finally:
                    self.db.execute("DEALLOCATE ALL")

    def test_materialized_relation_replay_preserves_self_join_multiplicity(self):
        with self.db.transaction(force_rollback=True):
            self.db.execute("CREATE TABLE items(n bigint)")
            self.db.execute("INSERT INTO items SELECT generate_series(0,4095)")
            self.assertEqual(
                self.db.execute(
                    "WITH cached AS MATERIALIZED (SELECT n FROM items) "
                    "SELECT count(*) FROM cached a JOIN cached b ON a.n=b.n"
                ).fetchone(),
                (4096,),
            )

    def test_typed_array_scalar_expression_contracts(self):
        import json
        from pathlib import Path

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_expression_reference.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["entries"]), 164)
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                with self.db.transaction(force_rollback=True):
                    self.db.execute("SET TRANSACTION READ ONLY")
                    self.assertEqual(
                        self.db.execute("SELECT " + case["sql"]).fetchone(),
                        (case["value"],),
                    )

    def test_typed_array_cast_rejection_contracts(self):
        import json
        from pathlib import Path

        import psycopg

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_cast_errors.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["entries"]), 45)
        for case in fixture["entries"]:
            with self.subTest(sql=case["sql"]):
                with self.assertRaises(psycopg.Error) as caught:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute("SET TRANSACTION READ ONLY")
                        self.db.execute("SELECT " + case["sql"])
                self.assertEqual(caught.exception.sqlstate, case["code"])

    def test_typed_array_text_input_contracts(self):
        import json
        from pathlib import Path

        import psycopg

        fixture = json.loads(
            (
                Path(__file__).resolve().parents[1]
                / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_text_reference.json"
            ).read_text()
        )
        self.assertEqual(len(fixture["entries"]), 19)
        self.assertEqual(len(fixture["errors"]), 19)
        types = {
            "text",
            "int2",
            "int4",
            "int8",
            "real",
            "float8",
            "bool",
            "uuid",
            "jsonb",
        }
        for case in fixture["entries"]:
            with self.subTest(input=case["input"], type=case["sql_type"]):
                self.assertIn(case["sql_type"], types)
                with self.db.transaction(force_rollback=True):
                    self.assertEqual(
                        self.db.execute(
                            "SELECT encode(array_send(%s::"
                            + case["sql_type"]
                            + "[]),'hex')",
                            (case["input"],),
                        ).fetchone()[0],
                        case["binary"],
                    )
        for case in fixture["errors"]:
            with self.subTest(input=case["input"], type=case["sql_type"]):
                self.assertIn(case["sql_type"], types)
                with self.assertRaises(psycopg.Error) as caught:
                    with self.db.transaction(force_rollback=True):
                        self.db.execute(
                            "SELECT %s::" + case["sql_type"] + "[]", (case["input"],)
                        )
                self.assertEqual(caught.exception.sqlstate, case["code"])

    def test_typed_array_binary_receive_boundary_admission(self):
        import struct

        import psycopg
        from psycopg.adapt import Dumper
        from psycopg.pq import Format

        class Payload:
            def __init__(self, data):
                self.data = data

        cases = [
            (
                1000,
                struct.pack("!iiIiiiB", 1, 0, 16, 1, 1, 1, 2),
                ("[1:1]", 1, "{t}"),
                None,
            ),
            (
                1007,
                struct.pack("!iiIiiii", 1, 0, 23, 1, 2147483647, 4, 1),
                None,
                "54000",
            ),
            (
                1007,
                struct.pack("!iiIiiii", 1, 0, 23, 1, 2147483646, 4, 1),
                ("[2147483646:2147483646]", 1, "[2147483646:2147483646]={1}"),
                None,
            ),
            (
                1007,
                struct.pack("!iiIiiii", 2, 0, 23, 100000, 1, 0, 1),
                (None, 0, "{}"),
                None,
            ),
            (
                1007,
                struct.pack("!iiIiii", 1, 0, 23, 1, 1, -1),
                ("[1:1]", 1, "{NULL}"),
                None,
            ),
            (1009, struct.pack("!iiIiii", 1, 0, 25, 1, 1, 1) + b"\0", None, "22021"),
        ]
        for oid, data, expected, sqlstate in cases:
            with self.subTest(oid=oid, payload=data.hex()):
                dumper = type(
                    "TypedArrayDumper",
                    (Dumper,),
                    {
                        "oid": oid,
                        "format": Format.BINARY,
                        "dump": lambda self, obj: obj.data,
                    },
                )
                self.db.adapters.register_dumper(Payload, dumper)
                try:
                    with self.db.transaction(force_rollback=True):
                        with psycopg.RawCursor(self.db) as cursor:
                            cursor.execute(
                                "SELECT array_dims($1), cardinality($1), $1::text",
                                [Payload(data)],
                            )
                            if sqlstate is not None:
                                self.fail(
                                    "PostgreSQL unexpectedly admitted invalid binary data"
                                )
                            self.assertEqual(cursor.fetchone(), expected)
                except psycopg.Error as error:
                    self.assertEqual(error.sqlstate, sqlstate)

    def test_exact_raw_parameter_reuse_bigint_and_json_null_provenance(self):
        result = execute(
            self.db,
            self.case(
                "SELECT $1::bigint AS exact, $1::bigint AS repeated, NULL::text AS missing, 'null'::jsonb AS json_null",
                [{"integer": "9007199254740993"}],
            ),
            read=True,
        )
        self.assertEqual(
            [[9007199254740993, 9007199254740993, None, None]], result["rows"]
        )
        self.assertEqual([[False, False, True, False]], result["sql_nulls"])
        self.assertEqual([20, 20, 25, 3802], result["column_oids"])

    def test_default_null_order_and_unicode_are_postgres_owned(self):
        result = execute(
            self.db,
            self.case(
                "SELECT value FROM (VALUES ('é'), (NULL::text), ('a')) AS v(value) ORDER BY value"
            ),
            read=True,
        )
        self.assertEqual([["a"], ["é"], [None]], result["rows"])
        result = execute(
            self.db,
            self.case(
                "SELECT bit_length('é') AS bits, strpos('aé🍎z','🍎') AS position, concat_ws(':','a',NULL,'',3) AS combined"
            ),
            read=True,
        )
        self.assertEqual([[16, 3, "a::3"]], result["rows"])
        result = execute(
            self.db,
            self.case(
                "SELECT lpad('é',4,'🍎x'),rpad('é',4,'🍎x'),lpad('é🍎',1,'x'),repeat('é🍎',2),reverse('aé🍎')"
            ),
            read=True,
        )
        self.assertEqual([["🍎x🍎é", "é🍎x🍎", "é", "é🍎é🍎", "🍎éa"]], result["rows"])

    def test_read_only_oracle_cannot_mutate_and_failed_case_does_not_poison_next(self):
        result = read_reference(
            self.db,
            [
                self.case("DELETE FROM usage_records RETURNING id"),
                self.case("SELECT id FROM public.usage_records"),
            ],
            self.profile(),
        )
        self.assertEqual(1, len(result["excluded"]))
        self.assertEqual([[9007199254740993]], result["entries"][0]["rows"])
        self.assertIsNone(
            self.db.execute("SELECT to_regclass('public.usage_records')").fetchone()[0]
        )

    def test_sqlite_only_or_emulated_functions_do_not_receive_postgres_credit(self):
        result = read_reference(
            self.db,
            [
                self.case("SELECT ends_with('abc','c') FROM usage_records"),
                self.case("SELECT id FROM usage_records"),
            ],
            self.profile(),
        )
        self.assertEqual(1, len(result["excluded"]))
        self.assertEqual(1, len(result["entries"]))
        for sql in [
            "SELECT concat_ws(':') FROM usage_records",
            "SELECT concat() FROM usage_records",
        ]:
            invalid = read_reference(self.db, [self.case(sql)], self.profile())
            self.assertEqual([], invalid["entries"])
            self.assertIn("does not exist", invalid["excluded"][0]["reason"])

    def test_exact_json_parameter_is_not_passed_as_text(self):
        result = execute(
            self.db,
            self.case(
                "SELECT jsonb_typeof($1) AS kind, $1->>'source' AS source",
                [{"json": '{"source":"api"}'}],
            ),
            read=True,
        )
        self.assertEqual([["object", "api"]], result["rows"])

    def test_document_final_state_preserves_undeclared_and_untouched_data(self):
        schema = {
            "default_type": "doc",
            "storage_mode": "document",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "properties": {
                            "title": {"type": "text"},
                            "status": {"type": "keyword"},
                        }
                    }
                }
            },
        }
        case = self.case(
            "UPDATE docs SET title='Changed' WHERE _id='doc:a' RETURNING _id,title"
        )
        result = document_reference(self.db, [case], {case["id"]: schema})
        self.assertEqual([], result["excluded"])
        entry = result["entries"][0]
        self.assertEqual([["doc:a", "Changed"]], entry["rows"])
        self.assertEqual(1, entry["affected"])
        expected = deepcopy(SEEDS)
        expected[0]["value"]["title"] = "Changed"
        self.assertEqual(expected, entry["final"])
        self.assertIsNone(
            self.db.execute("SELECT to_regclass('public.docs')").fetchone()[0]
        )

    def test_recursive_reference_is_resource_bounded(self):
        self.db.execute("SET statement_timeout = '50ms'")
        try:
            result = read_reference(
                self.db,
                [
                    self.case(
                        "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r) SELECT sum(x) FROM r"
                    )
                ],
                self.profile(),
            )
        finally:
            self.db.execute("SET statement_timeout = '2s'")
        self.assertEqual([], result["entries"])
        reason = result["excluded"][0]["reason"]
        self.assertTrue(
            "statement timeout" in reason or "temp_file_limit" in reason, reason
        )

    def test_large_reads_use_a_bounded_server_cursor_and_close_it_on_rejection(self):
        with patch(
            "psycopg.RawCursor",
            side_effect=AssertionError("read must not buffer the entire result"),
        ):
            with self.assertRaisesRegex(ValueError, "row budget"):
                execute(
                    self.db,
                    self.case("SELECT generate_series(1,100000000) AS n"),
                    read=True,
                )
        self.assertEqual(
            0,
            self.db.execute(
                "SELECT count(*) FROM pg_cursors WHERE name='antfly_reference'"
            ).fetchone()[0],
        )

    def test_explicit_null_assignment_is_not_confused_with_missing_property(self):
        schema = {
            "default_type": "doc",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "properties": {
                            "title": {"type": "text"},
                            "archived_at": {"type": "keyword"},
                        }
                    }
                }
            },
        }
        case = self.case("UPDATE docs SET archived_at=NULL WHERE _id='doc:a'")
        result = document_reference(self.db, [case], {case["id"]: schema})
        entry = result["entries"][0]
        self.assertIn("archived_at", entry["final"][0]["value"])
        self.assertIsNone(entry["final"][0]["value"]["archived_at"])
        self.assertNotIn("archived_at", entry["final"][1]["value"])

    def test_current_document_profile_retains_source_schema_without_granting_proofs(
        self,
    ):
        schema = {
            "default_type": "doc",
            "document_schemas": {
                "doc": {
                    "schema": {
                        "properties": {
                            "title": {"type": "text"},
                            "metadata": {"type": "json"},
                        }
                    }
                }
            },
        }
        case = self.case(
            "UPDATE docs SET title='Changed' WHERE metadata->>'source'='api'"
        )
        result = document_reference(self.db, [case], {case["id"]: schema})
        entry = result["entries"][0]
        self.assertEqual(
            "json",
            schema["document_schemas"]["doc"]["schema"]["properties"]["metadata"][
                "type"
            ],
        )
        self.assertEqual(schema, entry["schema"])
        self.assertEqual(
            "object",
            entry["native_schema"]["document_schemas"]["doc"]["schema"]["properties"][
                "metadata"
            ]["type"],
        )


class OrderingContractTest(unittest.TestCase):
    def test_ordered_peer_frontier_accepts_ties_but_not_worse_or_duplicate_rows(self):
        entry = {
            "rows": [["first"], ["peer-b"]],
            "sql_nulls": [[False], [False]],
            "ordered_groups": [
                {"rows": [["first"]], "sql_nulls": [[False]]},
                {"rows": [["peer-a"], ["peer-b"]], "sql_nulls": [[False], [False]]},
                {"rows": [["worse"]], "sql_nulls": [[False]]},
            ],
        }
        validate_ordered_groups(entry)
        for rows in [
            [["first"], ["worse"]],
            [["peer-a"], ["peer-b"]],
            [["first"], ["first"]],
        ]:
            with self.assertRaises(ValueError):
                validate_ordered_groups({**entry, "rows": rows})


if __name__ == "__main__":
    unittest.main()
