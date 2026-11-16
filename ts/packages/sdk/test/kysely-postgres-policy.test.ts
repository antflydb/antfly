// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import pg from "pg";
import { expect, it, vi } from "vitest";
import { createManagedPostgresTypes } from "../src/kysely-postgres-policy.js";

const cases = [
  [16, 1000, "t", true],
  [20, 1016, "9007199254740993", "9007199254740993"],
  [21, 1005, "-123", -123],
  [23, 1007, "2147483647", 2147483647],
  [700, 1021, "1.25", 1.25],
  [701, 1022, "Infinity", Infinity],
  [25, 1009, "hello", "hello"],
  [1043, 1015, "varchar", "varchar"],
  [1042, 1014, "fixed", "fixed"],
  [2950, 2951, "00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000000"],
] as const;

it.each([
  "exact",
  "pg-default",
] as const)("isolates all %s generated value families from global parser mutations", (values) => {
  const oids = [...cases.flatMap((entry) => entry.slice(0, 2) as number[]), 1700, 1231];
  const previous = oids.map((oid) => [oid, pg.types.getTypeParser(oid, "text")] as const);
  try {
    for (const oid of oids) pg.types.setTypeParser(oid, () => "wrong before construction");
    const types = createManagedPostgresTypes(pg.types, values);
    for (const oid of oids) pg.types.setTypeParser(oid, () => "wrong after construction");
    for (const [scalar, array, input, expected] of cases) {
      const scalarParser = types.getTypeParser(scalar) as (value: string) => unknown;
      const arrayParser = types.getTypeParser(array) as (value: string) => unknown;
      expect(scalarParser(input)).toEqual(expected);
      expect(arrayParser(`{{${input},NULL},{NULL,${input}}}`)).toEqual([
        [expected, null],
        [null, expected],
      ]);
      expect(() => types.getTypeParser(scalar, "binary")).toThrow("text results");
      expect(() => types.getTypeParser(array, "binary")).toThrow("text results");
    }
    expect((types.getTypeParser(1700) as (value: string) => unknown)("1.2300")).toBe("1.2300");
    const numeric = types.getTypeParser(1231) as (value: string) => unknown;
    expect(numeric("[0:1][2:3]={{1.2300,NULL},{NaN,-Infinity}}")).toEqual(
      values === "exact"
        ? [
            ["1.2300", null],
            ["NaN", "-Infinity"],
          ]
        : [
            [1.23, null],
            [NaN, -Infinity],
          ]
    );
    const strings = types.getTypeParser(1009) as (value: string) => unknown;
    expect(strings('{"NULL","a,b","a\\\\b","a\\"b",NULL}')).toEqual([
      "NULL",
      "a,b",
      "a\\b",
      'a"b',
      null,
    ]);
    expect(strings("{}")).toEqual([]);
  } finally {
    for (const [oid, parser] of previous) pg.types.setTypeParser(oid, parser);
  }
});

it("retains custom codecs for types generated as unknown", () => {
  const parser = vi.fn();
  const base = { getTypeParser: vi.fn(() => parser) };
  const types = createManagedPostgresTypes(base, "exact");
  expect(types.getTypeParser(3802, "text")).toBe(parser);
  expect(types.getTypeParser(1184, "binary")).toBe(parser);
  expect(base.getTypeParser).toHaveBeenCalledWith(3802, "text");
  expect(base.getTypeParser).toHaveBeenCalledWith(1184, "binary");
});
