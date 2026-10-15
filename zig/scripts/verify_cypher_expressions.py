#!/usr/bin/env python3
"""Qualify expression compatibility against authenticated native SQL storage."""

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18591")
    args = parser.parse_args()
    authorization = (
        "Basic "
        + base64.b64encode(os.environ["ANTFLY_CYPHER_TEST_AUTH"].encode()).decode()
    )
    table = "cypher_expressions_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_table = created_graph = False
    checks = 0

    def call(path, body=None, *, method="POST", status=None, retry_aborted=False):
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
                code, raw = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, raw = error.code, error.read()
        if retry_aborted and code == 409:
            failure = json.loads(raw)
            # Only the explicit noncommit diagnostic permits fixture replay.
            # An unknown outcome (40003) must never take this path.
            if failure.get("code") == "40001" and "was not committed" in failure.get(
                "message", ""
            ):
                for _ in range(30):
                    time.sleep(0.2)
                    try:
                        return call(path, body, method=method, status=status)
                    except AssertionError as retry_error:
                        detail = retry_error.args[0]
                        if not isinstance(detail, tuple) or detail[1] != 409:
                            raise
                        diagnostic = json.loads(detail[2])
                        if diagnostic.get(
                            "code"
                        ) != "40001" or "was not committed" not in diagnostic.get(
                            "message", ""
                        ):
                            raise
        assert (200 <= code < 300) if status is None else code == status, (
            path,
            code,
            raw.decode(),
        )
        return json.loads(raw) if raw else None

    def query(statement, **options):
        result = call(f"/graphs/{graph}/query", {"query": statement, **options})
        assert result["complete"], result
        return result

    def strings(result):
        return [[value["value"] for value in row] for row in result["rows"]]

    try:
        call(
            "/sql",
            {
                "statement": f"CREATE TABLE {table} (id TEXT,title TEXT,rank BIGINT,note TEXT,payload JSONB)"
            },
        )
        created_table = True
        call(
            "/sql",
            {
                "statement": f"INSERT INTO {table} (id,title,rank,payload) VALUES ('a','Alpha',1,'{{\"value\":null}}'::JSONB),('b','Beta',2,NULL),('c','Gamma',3,NULL)"
            },
            retry_aborted=True,
        )
        call(f"/graphs/{graph}", {"labels": [{"name": "Document", "table": table}]})
        created_graph = True
        # Computed data must stay bounded through comparison, delivery, and
        # write rollback, including when reduce builds shared containers.
        result = query("WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a RETURN a=a")
        assert result["rows"] == [[{"type": "boolean", "value": True}]], result
        assert result["stats"]["intermediate_states"] < 1000, result["stats"]
        checks += 1
        text = "x" * 400000
        cached_text = query(
            "RETURN reduce(a=0,i IN range(1,1000)|a+size(ltrim($s)))",
            parameters={"s": text},
            limits={"intermediate_states": 30000},
        )
        assert strings(cached_text) == [["400000000"]], cached_text
        checks += 1
        scanned_text = call(
            f"/graphs/{graph}/query",
            {
                "query": "RETURN reduce(a=0,i IN range(1,1000)|a+CASE WHEN $s CONTAINS 'z' THEN 1 ELSE 0 END)",
                "parameters": {"s": text},
                "limits": {"intermediate_states": 30000},
            },
            status=429,
        )
        assert scanned_text["code"] == "CypherStateBudgetExceeded", scanned_text
        checks += 1
        for statement, status, code in (
            (
                "RETURN reduce(a=[],x IN range(1,10000)|[a])",
                400,
                "CypherValueDepthExceeded",
            ),
            (
                "WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a,reduce(b=[1],x IN range(1,40)|[b,b]) AS b RETURN a=b",
                429,
                "CypherStateBudgetExceeded",
            ),
            (
                "CREATE (n:Document {id:'bounded_rollback'}) WITH n,reduce(a=[],x IN range(1,10000)|[a]) AS a RETURN a",
                400,
                "CypherValueDepthExceeded",
            ),
        ):
            failure = call(
                f"/graphs/{graph}/query", {"query": statement}, status=status
            )
            assert failure["code"] == code, failure
            checks += 1
        assert (
            query("MATCH (n:Document {id:'bounded_rollback'}) RETURN n")["rows"] == []
        )
        checks += 1
        for statement, rows in (
            ("UNWIND range(1,4000) AS x RETURN x,count(*)", 4000),
            (
                "UNWIND range(9223372036854771800,9223372036854775799) AS x RETURN x,count(*)",
                4000,
            ),
            ("UNWIND range(1,4000) AS x RETURN x UNION RETURN 0 AS x", 4001),
        ):
            result = query(statement)
            assert len(result["rows"]) == rows, result["stats"]
            assert result["stats"]["intermediate_states"] < 100000, result["stats"]
            checks += 1
        assert strings(
            query(
                "MATCH (n:Document) WITH n.title AS title WHERE n.rank>1 RETURN title ORDER BY title"
            )
        ) == [["Beta"], ["Gamma"]]
        checks += 1
        assert strings(
            query("MATCH (n:Document) WHERE n.title =~ '(?i)a.*' RETURN n.id")
        ) == [["a"]]
        checks += 1
        assert strings(
            query(
                "MATCH (n:Document) WHERE n.title =~ $pattern RETURN n.id ORDER BY n.id",
                parameters={"pattern": "Beta|Gamma"},
            )
        ) == [["b"], ["c"]]
        checks += 1
        assert query("RETURN $value =~ '.*'", parameters={"value": 1})["rows"] == [
            [{"type": "null", "value": None}]
        ]
        checks += 1
        properties = query(
            "MATCH (n:Document {id:'a'}) RETURN keys(n),n.note,n.payload"
        )["rows"][0]
        assert "note" not in {value["value"] for value in properties[0]["value"]}, (
            properties
        )
        assert properties[1] == {"type": "null", "value": None}, properties
        assert properties[2] == {
            "type": "map",
            "value": {"value": {"type": "null", "value": None}},
        }, properties
        checks += 1
        invalid = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (n:Document {id:'missing'}) WHERE n.title =~ '(' RETURN n"
            },
            status=400,
        )
        assert invalid["code"] == "CypherInvalidRegex", invalid
        checks += 1
        limited = call(
            f"/graphs/{graph}/query",
            {"query": "RETURN 'abc' =~ 'a.*'", "limits": {"intermediate_states": 10}},
            status=429,
        )
        assert limited["code"] == "CypherStateBudgetExceeded", limited
        checks += 1
        sql = call(
            "/sql",
            {
                "statement": f"SELECT g.id FROM GRAPH_TABLE({graph} MATCH (n IS Document) COLUMNS(n.id AS id,n.title AS title)) g WHERE g.title ~ '^A'"
            },
        )
        assert sql["rows"] == [["a"]], sql
        checks += 1
        assert strings(
            query(
                "RETURN toLower($text),toUpper($text),trim($space),ltrim($space),rtrim($space),trim($nbsp)",
                parameters={
                    "text": "ÉCOLE Straße ΟΣ 𐐀",
                    "space": "\u2003hi\u3000",
                    "nbsp": "\u00a0hi\u202f",
                },
            )
        ) == [
            [
                "école straße ος 𐐨",
                "ÉCOLE STRASSE ΟΣ 𐐀",
                "hi",
                "hi\u3000",
                "\u2003hi",
                "\u00a0hi\u202f",
            ]
        ]
        checks += 1
        written = query(
            "CREATE (n:Document {id:'unicode',title:'ÉCOLE'}) WITH n WHERE toLower(n.title)='école' SET n.note=toUpper(trim($note)) RETURN n.note",
            parameters={"note": "\u2003Straße\u3000"},
        )
        assert strings(written) == [["STRASSE"]], written
        assert strings(
            query("MATCH (n:Document) WHERE toLower(n.title)='école' RETURN n.note")
        ) == [["STRASSE"]]
        checks += 1
        sql = call(
            "/sql",
            {
                "statement": f"SELECT g.title FROM GRAPH_TABLE({graph} MATCH (n IS Document) WHERE lower(n.title)='école' COLUMNS(upper(n.title) AS title)) g"
            },
        )
        assert sql["rows"] == [["ÉCOLE"]], sql
        checks += 1
        for value in (42, True, "text", [1]):
            invalid = call(
                f"/graphs/{graph}/query",
                {
                    "query": "CREATE (:Document {id:'rollback'}) WITH $value AS x RETURN x.foo",
                    "parameters": {"value": value},
                },
                status=400,
            )
            assert invalid["code"] == "CypherTypeMismatch", invalid
            checks += 1
        for text in (
            "9223372036854775808",
            "-9223372036854775809",
            "9223372036854775807.1",
            "-9223372036854775808.1",
        ):
            invalid = call(
                f"/graphs/{graph}/query",
                {
                    "query": "CREATE (:Document {id:'rollback'}) RETURN toInteger($value)",
                    "parameters": {"value": text},
                },
                status=400,
            )
            assert invalid["code"] == "CypherNumericOverflow", invalid
            checks += 1
        assert strings(query("MATCH (n:Document {id:'rollback'}) RETURN count(n)")) == [
            ["0"]
        ]
        checks += 1
        assert strings(
            query(
                "RETURN toInteger('9007199254740993.0'),toInteger('9223372036854775807.0'),toInteger('-9223372036854775808.0'),toString(1.0),toString(1e20)"
            )
        ) == [
            [
                "9007199254740993",
                "9223372036854775807",
                "-9223372036854775808",
                "1.0",
                "1.0E20",
            ]
        ]
        checks += 1
        for values, expected in (
            (
                "[9223372036854775807,9223372036854775807]",
                "toFloat(9223372036854775807)",
            ),
            ("[1e308,1e308]", "1e308"),
            ("[1e308,-1e308]", "0.0"),
        ):
            assert strings(query(f"UNWIND {values} AS x RETURN avg(x)={expected}")) == [
                [True]
            ]
            checks += 1
        distinct = query("UNWIND [1,1.0] AS x RETURN collect(DISTINCT toString(x))")[
            "rows"
        ][0][0]
        assert distinct == {
            "type": "list",
            "value": [
                {"type": "string", "value": "1"},
                {"type": "string", "value": "1.0"},
            ],
        }, distinct
        checks += 1
        sql = call(
            "/sql",
            {
                "statement": f"SELECT g.value,g.text FROM GRAPH_TABLE({graph} MATCH (n IS Document) WHERE n.id='a' COLUMNS(toInteger('9007199254740993.0') AS value,toString(1.0) AS text)) g"
            },
        )
        assert sql["rows"] == [["9007199254740993", "1.0"]], sql
        checks += 1
        for prefix in ("", "EXPLAIN "):
            empty = query(f"{prefix}MATCH (n:Document) WHERE false RETURN *")
            assert empty["columns"] == ["n"] and empty["rows"] == [], empty
            checks += 1
        for expression, expected in (
            ("trim(' a ')", "a!"),
            ("substring('abc',1,1)", "b!"),
            ("reverse('ab')", "ba!"),
            ("coalesce(null,'a')", "a!"),
        ):
            sql = call(
                "/sql",
                {
                    "statement": f"SELECT g.x || '!' FROM GRAPH_TABLE({graph} MATCH (n IS Document) WHERE n.id='a' COLUMNS({expression} AS x)) g"
                },
            )
            assert sql["rows"] == [[expected]], sql
            checks += 1
        sql = call(
            "/sql",
            {
                "statement": f"SELECT g.x + 1 FROM GRAPH_TABLE({graph} MATCH (n IS Document) WHERE n.id='a' COLUMNS(abs(-2) AS x,toBoolean('true') AS ok)) g WHERE g.ok"
            },
        )
        assert sql["rows"] == [["3"]], sql
        checks += 1
        for statement, expected in (
            (
                "RETURN toBoolean(0),toBoolean(-1),toBoolean(' true '),toBoolean(' false ')",
                [[False, True, True, False]],
            ),
            (
                "RETURN toFloat(' 1.5 '),toFloat('1.5d'),toFloat('0x1.8p1')",
                [[1.5, 1.5, 3]],
            ),
            ("RETURN 'row-'+1,1+'-row','v='+true", [["row-1", "1-row", "v=true"]]),
            (
                "MATCH (n:Document {id:n.id}) RETURN n.id ORDER BY n.id",
                [["a"], ["b"], ["c"], ["unicode"]],
            ),
            (
                "MATCH (a:Document {id:b.id}),(b:Document) RETURN a.id,b.id ORDER BY a.id",
                [["a", "a"], ["b", "b"], ["c", "c"], ["unicode", "unicode"]],
            ),
        ):
            actual = strings(query(statement))
            assert actual == expected, (statement, actual, expected)
            checks += 1
        sql = call(
            "/sql",
            {
                "statement": f"SELECT g.x || '!' FROM GRAPH_TABLE({graph} MATCH (n IS Document) WHERE n.id='a' COLUMNS('row-'+n.rank AS x,toBoolean(n.rank) AS ok)) g WHERE g.ok"
            },
        )
        assert sql["rows"] == [["row-1!"]], sql
        checks += 1
        print(f"{checks} native Cypher/SQL expression checks passed")
    finally:
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            call("/sql", {"statement": f"DROP TABLE {table}"})


if __name__ == "__main__":
    main()
