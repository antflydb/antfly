import { cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { TableSchema } from "../api";
import CreateIndexDialog, {
  buildGraphSourceConfig,
  getSchemaFieldNames,
} from "./CreateIndexDialog";

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

  it("normalizes graph source UX fields into the public multi-source contract", () => {
    expect(
      buildGraphSourceConfig({
        artifact: " relations_v1 ",
        path: " $.relations[*] ",
        format: "extraction_relation",
        mentionEdgeType: " mentions ",
        nodeModel: "external",
        sourceNode: " {{ _doc.key }} ",
        targetNode: " {{ _item.target.text }} ",
        edgeType: " {{ _item.predicate }} ",
        contextFields: "title, body",
      })
    ).toEqual({
      artifact: "relations_v1",
      path: "$.relations[*]",
      format: "extraction_relation",
      mention_edge_type: "mentions",
      nodes: {
        model: "external",
        source: "{{ _doc.key }}",
        target: "{{ _item.target.text }}",
      },
      edge: { type: "{{ _item.predicate }}" },
      context: { doc_fields: ["title", "body"] },
    });
  });

  it("preserves an external graph node model without custom templates", () => {
    expect(
      buildGraphSourceConfig({
        artifact: "external_relations_v1",
        path: "",
        format: "extraction_graph",
        mentionEdgeType: "",
        nodeModel: "external",
        sourceNode: "",
        targetNode: "",
        edgeType: "",
        contextFields: "",
      })
    ).toEqual({
      artifact: "external_relations_v1",
      format: "extraction_graph",
      nodes: { model: "external" },
    });
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
