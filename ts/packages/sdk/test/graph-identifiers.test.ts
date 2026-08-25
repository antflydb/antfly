// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { describe, expect, it } from "vitest";
import {
  GRAPH_IDENTIFIER_CONFORMANCE_CASES,
  isValidGraphIdentifier,
} from "../src/graph-identifier-policy.generated.js";
import { validateGraphQueryIdentifiers } from "../src/graph-identifiers.js";

describe("graph identifier policy", () => {
  for (const testCase of GRAPH_IDENTIFIER_CONFORMANCE_CASES) {
    it(testCase.name, () => {
      expect(isValidGraphIdentifier(testCase.value)).toBe(testCase.valid);
    });
  }

  it("rejects unsafe aliases before sending a query", () => {
    expect(() =>
      validateGraphQueryIdentifiers({
        people: {
          match: {
            anchor: "person",
            nodes: { person: {}, "post\u200bauthor": {} },
            edges: [{ from: "person", to: "post\u200bauthor" }],
          },
          return: { bindings: ["person"] },
        },
      })
    ).toThrow("match.nodes key");
  });

  it("matches the server predicate-depth limit", () => {
    let where: Record<string, unknown> = {
      not_equal: { left: { alias: "person" }, right: { alias: "author" } },
    };
    for (let depth = 0; depth < 16; depth += 1) where = { and: [where] };

    expect(() =>
      validateGraphQueryIdentifiers({
        people: {
          match: {
            anchor: "person",
            nodes: { person: {}, author: {} },
            edges: [{ from: "person", to: "author" }],
            where,
          },
          return: { bindings: ["person"] },
        },
      })
    ).toThrow("maximum graph predicate depth");
  });
});
