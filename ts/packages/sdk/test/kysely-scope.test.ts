// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { fileURLToPath } from "node:url";
import ts from "typescript";
import { expect, it, vi } from "vitest";
import {
  type AntflyKyselyClient,
  createAntflyIntrospector,
  generateKyselyTypes,
} from "../src/kysely.js";

function catalog() {
  const GET = vi.fn(
    async (
      path: string,
      options: { params: { path: { namespaceName?: string }; query?: { cursor?: string } } }
    ) => {
      let data: unknown;
      let cursor: string | undefined;
      if (path.endsWith("/namespaces")) data = [{ name: "app" }, { name: "public" }];
      else if (path.endsWith("/tables")) {
        // Empty authorization-filtered pages must still advance through the cursor.
        if (options.params.path.namespaceName === "app" && !options.params.query?.cursor) {
          data = [];
          cursor = "next";
        } else data = [{ name: "items" }];
      } else
        data = {
          schema: {
            document_schemas: {
              row: { schema: { required: ["value"], properties: { value: { type: "string" } } } },
            },
          },
        };
      return {
        data,
        response: new Response(null, { headers: cursor ? { "X-Antfly-Next-Cursor": cursor } : {} }),
      };
    }
  );
  const executeSQL = vi.fn(async (request: { namespace?: string }) => ({
    columns: [
      {
        name: "value",
        type: request.namespace === "app" ? "string" : "integer",
        element_type: request.namespace === "app" ? "text" : "int4",
      },
    ],
  }));
  return { client: { api: { GET }, executeSQL } as unknown as AntflyKyselyClient, GET, executeSQL };
}

it("enumerates namespace metadata and descriptions in the same explicit database scope", async () => {
  const { client, GET, executeSQL } = catalog();
  const introspector = createAntflyIntrospector(client, { database: "tenant" });
  expect(await introspector.getSchemas()).toEqual([{ name: "app" }, { name: "public" }]);
  const tables = await introspector.getTables();
  expect(tables.map((table) => `${table.schema}.${table.name}`)).toEqual([
    "app.items",
    "public.items",
  ]);
  expect(
    GET.mock.calls.every(
      ([, options]) => (options.params.path as { databaseName?: string }).databaseName === "tenant"
    )
  ).toBe(true);
  expect(executeSQL).toHaveBeenCalledWith({
    statement: 'SELECT * FROM "items" LIMIT 0',
    database: "tenant",
    namespace: "app",
  });
  const source = generateKyselyTypes(tables, {
    integerMode: "string",
    searchPath: ["app", "public"],
  });
  expect(source).toContain('"app.items": {\n    "value": string;');
  expect(source).toContain('"public.items": {\n    "value": number;');
  expect(source).toContain('"items": {\n    "value": string;');
  const fileName = fileURLToPath(new URL("../type-test/generated-scope.ts", import.meta.url));
  const input = `${source}
import type { Kysely } from "kysely";
declare const db: Kysely<Database>;
export const unqualified: Promise<{ value: string }[]> = db.selectFrom("items").selectAll().execute();
export const qualified: Promise<{ value: number }[]> = db.selectFrom("public.items").selectAll().execute();
// @ts-expect-error Qualified table must retain its own field type, not its shadow's.
db.selectFrom("public.items").where("value", "=", "string");
`;
  const compilerOptions: ts.CompilerOptions = {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.NodeNext,
    strict: true,
    noEmit: true,
    skipLibCheck: true,
    types: [],
  };
  const host = ts.createCompilerHost(compilerOptions);
  const original = host.getSourceFile.bind(host);
  host.getSourceFile = (name, version, onError, create) =>
    name === fileName
      ? ts.createSourceFile(name, input, version)
      : original(name, version, onError, create);
  expect(
    ts
      .getPreEmitDiagnostics(ts.createProgram([fileName], compilerOptions, host))
      .map((diagnostic) => ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n"))
  ).toEqual([]);
  const reversed = generateKyselyTypes(tables, {
    integerMode: "string",
    searchPath: ["public", "app"],
  });
  expect(reversed).toContain('"items": {\n    "value": number;');
});

it("can restrict introspection to selected namespaces without changing catalog schema discovery", async () => {
  const { client, GET } = catalog();
  const introspector = createAntflyIntrospector(client, {
    database: "tenant",
    namespaces: ["app"],
  });
  expect((await introspector.getTables()).map((table) => table.schema)).toEqual(["app"]);
  expect(
    GET.mock.calls
      .filter(([path]) => path.endsWith("/tables"))
      .every(([, options]) => options.params.path.namespaceName === "app")
  ).toBe(true);
});

it("fails closed on namespace catalog errors", async () => {
  const { client, GET } = catalog();
  GET.mockResolvedValue({ data: undefined, response: new Response(null, { status: 403 }) });
  await expect(
    createAntflyIntrospector(client, { database: "tenant" }).getTables()
  ).rejects.toThrow("HTTP 403");
});
