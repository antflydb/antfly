import { describe, expect, it } from "vitest";
import { buildCreateTableRequest, createTableErrorMessage } from "./create-table";

describe("buildCreateTableRequest", () => {
  it("lets Antfly install its defaults for a name-only table", () => {
    expect(buildCreateTableRequest(1, { version: 0, document_schemas: {} })).toEqual({
      num_shards: 1,
    });
  });

  it("keeps a configured schema but omits its backend-managed version", () => {
    const request = buildCreateTableRequest(3, {
      version: 7,
      default_type: "article",
      document_schemas: {
        article: {
          schema: {
            type: "object",
            properties: {
              title: { type: "string" },
            },
          },
        },
      },
    });

    expect(request).toEqual({
      num_shards: 3,
      schema: {
        default_type: "article",
        document_schemas: {
          article: {
            schema: {
              type: "object",
              properties: {
                title: { type: "string" },
              },
            },
          },
        },
      },
    });
    expect(request.schema).not.toHaveProperty("version");
  });
});

describe("createTableErrorMessage", () => {
  it("preserves API error details", () => {
    expect(createTableErrorMessage(new Error("Failed to create table: invalid request"))).toBe(
      "Failed to create table: invalid request"
    );
  });

  it("provides a fallback for non-Error failures", () => {
    expect(createTableErrorMessage(null)).toBe("Failed to create table.");
  });
});
