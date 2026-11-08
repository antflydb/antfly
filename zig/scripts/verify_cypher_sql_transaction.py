#!/usr/bin/env python3
"""Qualify native GRAPH_TABLE against staged SQL rows and derived adjacency."""

import argparse
import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18591")
    parser.add_argument("--pgport", type=int, default=18592)
    parser.add_argument("--psql", default="/opt/homebrew/opt/postgresql@18/bin/psql")
    parser.add_argument("--policy", action="store_true")
    args = parser.parse_args()
    user, password = os.environ["ANTFLY_CYPHER_TEST_AUTH"].split(":", 1)
    authorization = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
    table = "cypher_sql_txn_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_table = created_graph = enabled = False

    def call(path, body=None, method="POST", accepted=(200, 201, 202, 204)):
        request = urllib.request.Request(
            args.url.rstrip("/") + "/db/v1" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": authorization,
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=40) as response:
                status, content = response.status, response.read()
        except urllib.error.HTTPError as error:
            status, content = error.code, error.read()
        assert status in accepted, (path, status, content.decode())
        return json.loads(content) if content else None

    def sql(statement, session_id=None):
        body = {"statement": statement}
        if session_id is not None:
            body["session_id"] = session_id
        return call("/sql", body)

    def psql(statement):
        result = subprocess.run(
            [
                args.psql,
                f"host=127.0.0.1 port={args.pgport} user={user} dbname=default sslmode=disable",
                "-X",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=statement,
            text=True,
            capture_output=True,
            env={**os.environ, "PGPASSWORD": password},
            timeout=60,
        )
        assert result.returncode == 0, (result.stdout, result.stderr)
        return result.stdout.splitlines()

    def graph_nodes(marker):
        return f"SELECT '{marker}'; SELECT g.node FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS (n AS node)) g;"

    def section(lines, start, end):
        return lines[lines.index(start) + 1 : lines.index(end)]

    def nodes(lines):
        result = {}
        for line in lines:
            if not line.startswith("{"):
                continue
            node = json.loads(line)
            assert node["type"] == "node", node
            properties = node["value"]["properties"]["value"]
            result[properties["id"]["value"]] = properties["title"]["value"]
        return result

    try:
        sql(f"CREATE TABLE {table} (id TEXT, title TEXT, tenant TEXT, ref TEXT)")
        created_table = True
        call(
            f"/tables/{table}/indexes/links",
            {"type": "graph", "edge_types": [{"name": "cites", "field": "ref"}]},
        )
        sql(
            f"INSERT INTO {table} (id,title,tenant) VALUES ('a','Alpha','visible'),('b','Beta','visible'),('c','Gamma','visible')"
        )
        keys = {row[0]: row[1] for row in sql(f"SELECT id,_id FROM {table}")["rows"]}
        sql(
            f"UPDATE {table} SET ref='{keys['b'].replace(chr(39), chr(39) * 2)}' WHERE id='a'"
        )
        call(
            f"/graphs/{graph}",
            {
                "labels": [{"name": "Document", "table": table}],
                "relationships": [
                    {
                        "name": "CITES",
                        "index": "links",
                        "source_label": "Document",
                        "target_label": "Document",
                        "stored_type": "cites",
                    }
                ],
            },
        )
        created_graph = True
        if args.policy:
            sql(f"INSERT INTO {table} (id,title,tenant) VALUES ('h','Hidden','hidden')")
            sql(
                f"CREATE POLICY visible ON {table} USING (tenant='visible') WITH CHECK (true)"
            )
            sql(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
            enabled = True
            deadline = time.monotonic() + 30
            while True:
                result = call(
                    f"/graphs/{graph}/query",
                    {"query": "MATCH (n:Document) RETURN n.id ORDER BY n.id"},
                    accepted=(200, 403, 409, 503),
                )
                if result.get("rows") == [
                    [{"type": "string", "value": value}] for value in ("a", "b", "c")
                ]:
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(("policy publication did not settle", result))
                time.sleep(0.1)
        statements = f"""
BEGIN;
UPDATE {table} SET title='Changed' WHERE id='a';
INSERT INTO {table} (id,title,tenant) VALUES ('d','Delta','visible');
DELETE FROM {table} WHERE id='b';
{graph_nodes("STAGED_NODES")}
SELECT 'JOIN_ROWS';
SELECT t.id,t.title FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS (antfly.documentKey(n) AS key)) g JOIN {table} t ON t._id=g.key ORDER BY t.id;
SELECT 'STAGED_END';
ROLLBACK;
{graph_nodes("RESTORED_NODES")}
SELECT 'RESTORED_END';
BEGIN;
UPDATE {table} SET ref='{keys["c"].replace(chr(39), chr(39) * 2)}' WHERE id='a';
SELECT 'STAGED_EDGES';
SELECT g.source,g.target FROM GRAPH_TABLE({graph} MATCH (a IS Document)-[r IS CITES]->(b IS Document) COLUMNS (a AS source,b AS target)) g;
SELECT 'EDGES_END';
ROLLBACK;
BEGIN;
UPDATE {table} SET ref=NULL WHERE id='a';
SELECT 'NULL_EDGES';
SELECT g.source,g.target FROM GRAPH_TABLE({graph} MATCH (a IS Document)-[r IS CITES]->(b IS Document) COLUMNS (a AS source,b AS target)) g;
SELECT 'NULL_END';
ROLLBACK;
"""
        lines = psql(statements)
        assert nodes(section(lines, "STAGED_NODES", "JOIN_ROWS")) == {
            "a": "Changed",
            "c": "Gamma",
            "d": "Delta",
        }, lines
        assert section(lines, "JOIN_ROWS", "STAGED_END") == [
            "a|Changed",
            "c|Gamma",
            "d|Delta",
        ], lines
        assert nodes(section(lines, "RESTORED_NODES", "RESTORED_END")) == {
            "a": "Alpha",
            "b": "Beta",
            "c": "Gamma",
        }, lines
        assert section(lines, "NULL_EDGES", "NULL_END") == [], lines
        edges = section(lines, "STAGED_EDGES", "EDGES_END")
        assert len(edges) == 1, lines
        source, target = [json.loads(value) for value in edges[0].split("|")]
        assert source["value"]["properties"]["value"]["id"]["value"] == "a", source
        assert target["value"]["properties"]["value"]["id"]["value"] == "c", target
        # Explicit graph effects staged through the transaction API participate
        # in the same SQL graph cut as document postimages.
        edge = {
            "edge_id": "cypher:" + uuid.uuid4().hex,
            "index_name": "links",
            "source": keys["b"],
            "target": keys["c"],
            "edge_type": "cites",
            "metadata_json": json.dumps(
                {"note": "staged", "antfly_cypher": "customer-value"}
            ),
        }
        session_id = sql("BEGIN")["session_id"]
        try:
            # The native raw-effects contract authenticates the source through
            # its primary intent. Cypher builds this witness internally.
            source_witness = {
                keys["b"]: {"id": "b", "title": "Beta", "tenant": "visible"}
            }
            call(
                f"/transactions/{session_id}/stage",
                {
                    "read_set": [],
                    "tables": {
                        table: {"inserts": source_witness, "graph_writes": [edge]}
                    },
                },
            )
            count = sql(
                f"SELECT count(*) FROM GRAPH_TABLE({graph} MATCH (a IS Document)-[r IS CITES]->(b IS Document) COLUMNS (r AS edge)) g",
                session_id,
            )
            assert count["rows"] == [["2"]], count
            identity_query = f"SELECT g.edge FROM GRAPH_TABLE({graph} MATCH ()-[r IS CITES]->() WHERE r.note='staged' COLUMNS(r AS edge)) g"
            staged_edges = sql(identity_query, session_id)["rows"]
            assert len(staged_edges) == 1, staged_edges
            staged_edge = staged_edges[0][0]
            if isinstance(staged_edge, str):
                staged_edge = json.loads(staged_edge)
            assert (
                staged_edge["value"]["properties"]["value"]["antfly_cypher"]["value"]
                == "customer-value"
            ), staged_edge
            sql("COMMIT", session_id)
            session_id = None
            committed_edges = sql(identity_query)["rows"]
            committed_edge = committed_edges[0][0]
            if isinstance(committed_edge, str):
                committed_edge = json.loads(committed_edge)
            assert committed_edge == staged_edge, (committed_edge, staged_edge)
        finally:
            if session_id is not None:
                sql("ROLLBACK", session_id)
        session_id = sql("BEGIN")["session_id"]
        try:
            deletion = {
                name: value for name, value in edge.items() if name != "metadata_json"
            }
            call(
                f"/transactions/{session_id}/stage",
                {
                    "read_set": [],
                    "tables": {
                        table: {"inserts": source_witness, "graph_deletes": [deletion]}
                    },
                },
            )
            count = sql(
                f"SELECT count(*) FROM GRAPH_TABLE({graph} MATCH (a IS Document)-[r IS CITES]->(b IS Document) COLUMNS (r AS edge)) g",
                session_id,
            )
            assert count["rows"] == [["1"]], count
            sql("COMMIT", session_id)
            session_id = None
        finally:
            if session_id is not None:
                sql("ROLLBACK", session_id)
        # Later Cypher clauses see rebuilt field-derived adjacency as well.
        result = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (a:Document {id:'a'}) SET a.ref=$target WITH a MATCH (a)-[:CITES]->(b) RETURN b.id",
                "parameters": {"target": keys["c"]},
            },
        )
        assert result["rows"] == [[{"type": "string", "value": "c"}]], result
        result = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (a:Document {id:'a'}) REMOVE a.ref WITH a MATCH (a)-[:CITES]->(b) RETURN b.id"
            },
        )
        assert result["rows"] == [], result
        sql(
            f"UPDATE {table} SET ref='{keys['b'].replace(chr(39), chr(39) * 2)}' WHERE id='a'"
        )
        if args.policy:
            lines = psql(
                f"BEGIN; UPDATE {table} SET tenant='hidden' WHERE id='a'; {graph_nodes('HIDDEN_POSTIMAGE')} SELECT 'HIDDEN_END'; ROLLBACK;"
            )
            assert nodes(section(lines, "HIDDEN_POSTIMAGE", "HIDDEN_END")) == {
                "b": "Beta",
                "c": "Gamma",
            }, lines
        print(
            "Native SQL GRAPH_TABLE staged rows, joins, explicit graph effects, derived adjacency, rollback, and row authority passed; Cypher derived read-your-writes passed"
        )
    finally:
        if enabled:
            sql(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            sql(f"DROP TABLE {table}")


if __name__ == "__main__":
    main()
