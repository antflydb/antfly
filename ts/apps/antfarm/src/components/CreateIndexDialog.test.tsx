import { cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { TableSchema } from "../api";
import CreateIndexDialog, { getSchemaFieldNames } from "./CreateIndexDialog";

vi.mock("./IndexForm", () => ({
  default: ({ schemaFields }: { schemaFields: string[] }) => (
    <div data-testid="index-form">{schemaFields.join(",")}</div>
  ),
}));

describe("CreateIndexDialog", () => {
  afterEach(() => {
    cleanup();
  });

  it("derives schema fields from valid document schema properties", () => {
    const schema = {
      document_schemas: {
        dynamic: {
          schema: {
            type: "object",
          },
        },
        article: {
          schema: {
            type: "object",
            properties: {
              title: { type: "string" },
              body: { type: "string" },
            },
          },
        },
      },
    } as unknown as TableSchema;

    expect(getSchemaFieldNames(schema)).toEqual(["body", "title"]);
  });

  it("does not crash while closed when schema properties are absent", () => {
    const schema = {
      document_schemas: {
        dynamic: {
          schema: {
            type: "object",
          },
        },
      },
    } as unknown as TableSchema;

    expect(() =>
      render(
        <CreateIndexDialog
          open={false}
          onClose={() => undefined}
          tableName="montessori_copilot_ft"
          onIndexCreated={() => undefined}
          schema={schema}
        />
      )
    ).not.toThrow();
  });
});
