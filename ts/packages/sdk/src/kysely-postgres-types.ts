// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import type { PostgresCursor, PostgresCursorConstructor } from "kysely";

/** Structural pg type registry, without a runtime dependency on pg. */
export interface AntflyPostgresTypes {
  getTypeParser(oid: number, format?: "text" | "binary"): unknown;
}

/**
 * Pool-local exact NUMERIC parsers. Pass pg.types as the base registry, then
 * pass the result to both Pool's types option and createAntflyPostgresCursor.
 * Other OIDs and binary parsers retain the supplied registry's behavior.
 */
export function createAntflyPostgresTypes(base: AntflyPostgresTypes): AntflyPostgresTypes {
  // The text[] codec preserves quoting, nested dimensions, NULL elements, and
  // decimal spelling. pg's numeric[] codec instead calls parseFloat.
  const numericArray = base.getTypeParser(1009, "text");
  return {
    getTypeParser(oid, format = "text") {
      if (format === "text") {
        if (oid === 1700) return (value: string) => value;
        if (oid === 1231) return numericArray;
      }
      return base.getTypeParser(oid, format);
    },
  };
}

/** pg-cursor's constructor, including its per-cursor types option. */
export interface AntflyConfigurablePostgresCursor {
  new <T>(
    statement: string,
    parameters: unknown[],
    config: { types: AntflyPostgresTypes }
  ): PostgresCursor<T>;
}

/** pg-cursor does not inherit Pool's types option; bind the same registry here. */
export function createAntflyPostgresCursor(
  Cursor: AntflyConfigurablePostgresCursor,
  types: AntflyPostgresTypes
): PostgresCursorConstructor {
  return class<T> extends Cursor<T> {
    constructor(statement: string, parameters: unknown[]) {
      super(statement, parameters, { types });
    }
  };
}
