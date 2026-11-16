// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { parse } from "postgres-array";
import type { AntflyPostgresTypes } from "./kysely-postgres-types.js";

type TextParser = (value: string) => unknown;

/** Managed text codecs are independent of pg's process-global registry. */
export function createManagedPostgresTypes(
  base: AntflyPostgresTypes,
  values: "exact" | "pg-default"
): AntflyPostgresTypes {
  const text = (value: string) => value;
  const boolean = (value: string) => value === "t";
  // Every scalar family emitted concretely by generateKyselyTypes must be
  // represented here. Other OIDs retain the caller's codecs (and generate as unknown).
  const scalars: [number, number, TextParser][] = [
    [16, 1000, boolean], // bool
    [20, 1016, text], // int8
    [21, 1005, Number], // int2
    [23, 1007, Number], // int4
    [700, 1021, Number], // float4
    [701, 1022, Number], // float8
    [25, 1009, text], // text
    [1043, 1015, text], // varchar
    [1042, 1014, text], // bpchar
    [2950, 2951, text], // uuid
  ];
  const parsers = new Map<number, TextParser>();
  for (const [scalarOid, arrayOid, parser] of scalars) {
    parsers.set(scalarOid, parser);
    parsers.set(arrayOid, (value) => parse(value, parser));
  }
  parsers.set(1700, text); // numeric always retains decimal spelling
  parsers.set(1231, (value) => parse<unknown>(value, values === "exact" ? text : Number));
  return {
    getTypeParser(oid, format = "text") {
      const parser = parsers.get(oid);
      if (parser) {
        if (format !== "text") {
          throw new Error("Managed Antfly value policies require PostgreSQL text results");
        }
        return parser;
      }
      return base.getTypeParser(oid, format);
    },
  };
}
