import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { TableSchema } from "../api";
import CreateIndexDialog, {
  buildGraphEdgeTypeConfig,
  buildGraphSourceConfig,
  getSchemaFieldNames,
  parseAdvancedIndexConfig,
} from "./CreateIndexDialog";

vi.mock("./IndexForm", () => ({
  default: ({
    schemaFields,
    allowArtifactSources,
  }: {
    schemaFields: string[];
    allowArtifactSources: boolean;
  }) => (
    <div data-testid="index-form" data-artifact-sources={allowArtifactSources}>
      {schemaFields.join(",")}
    </div>
  ),
}));

describe("CreateIndexDialog", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
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
        edgeWeight: " {{ _item.confidence }} ",
        edgeMetadata: '{"evidence":"{{ _item.evidence }}"}',
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
      edge: {
        type: "{{ _item.predicate }}",
        weight: "{{ _item.confidence }}",
        metadata: { evidence: "{{ _item.evidence }}" },
      },
      context: { doc_fields: ["title", "body"] },
    });
  });

  it("normalizes direct document-field graph edge types", () => {
    expect(
      buildGraphEdgeTypeConfig({
        name: " citations ",
        field: " cited_ids ",
        topology: "tree",
        allowSelfLoops: false,
      })
    ).toEqual({
      name: "citations",
      field: "cited_ids",
      topology: "tree",
      allow_self_loops: false,
    });
  });

  it("parses complete advanced JSON while rejecting invalid top-level contracts", () => {
    expect(
      parseAdvancedIndexConfig(
        JSON.stringify({
          name: "knowledge_graph",
          type: "graph",
          sources: [{ artifact: "relations_v1" }],
          algebraic_planning: { bounded_traversal: { law: "provenance_semiring" } },
        })
      )
    ).toMatchObject({ name: "knowledge_graph", type: "graph" });
    expect(() => parseAdvancedIndexConfig("[]")).toThrow("must be a JSON object");
    expect(() => parseAdvancedIndexConfig('{"name":"missing_type"}')).toThrow("type must be");
    expect(() =>
      parseAdvancedIndexConfig(
        JSON.stringify({ name: "too_many", type: "graph", sources: Array(65).fill({}) })
      )
    ).toThrow("between 1 and 64");
    expect(() =>
      parseAdvancedIndexConfig(
        JSON.stringify({ name: "not_an_array", type: "full_text", sources: "chunks_v1" })
      )
    ).toThrow("must be an array");
    expect(() =>
      parseAdvancedIndexConfig(
        JSON.stringify({
          name: "duplicate_sources",
          type: "embeddings",
          sources: [{ artifact: "dense_v1" }, { artifact: "dense_v1" }],
        })
      )
    ).toThrow("duplicate artifact");
    expect(() =>
      parseAdvancedIndexConfig(
        JSON.stringify({
          name: "ambiguous_sources",
          type: "embeddings",
          sources: [{ artifact: "dense_v1" }],
          field: "body",
        })
      )
    ).toThrow("cannot be combined with field");
    expect(() =>
      parseAdvancedIndexConfig(
        JSON.stringify({
          name: "invalid_graph_source",
          type: "graph",
          sources: [{ artifact: "relations_v1", path: "relations[*]" }],
        })
      )
    ).toThrow("path must be");
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

  it("keeps live graph previews renderable while metadata JSON is incomplete", () => {
    expect(
      buildGraphSourceConfig({
        artifact: "relations_v1",
        path: "",
        format: "extraction_relation",
        nodeModel: "document",
        edgeMetadata: '{"evidence":',
      })
    ).toEqual({ artifact: "relations_v1", format: "extraction_relation" });
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

  it("does not silently discard Raw JSON edits when returning to the form", () => {
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        unobserve() {}
        disconnect() {}
      }
    );
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(false);
    render(
      <CreateIndexDialog
        open
        onClose={() => undefined}
        tableName="docs"
        onIndexCreated={() => undefined}
        schema={null}
      />
    );

    const modeSwitch = screen.getByRole("switch");
    fireEvent.click(modeSwitch);
    const editor = screen.getByLabelText("Advanced index JSON");
    fireEvent.change(editor, {
      target: { value: '{"name":"advanced","type":"graph","sources":[{"artifact":"relations"}]}' },
    });
    fireEvent.click(modeSwitch);

    expect(confirm).toHaveBeenCalledOnce();
    expect(screen.getByLabelText("Advanced index JSON")).toBeTruthy();

    confirm.mockReturnValue(true);
    fireEvent.click(modeSwitch);
    expect(screen.getByTestId("index-form")).toBeTruthy();
  });

  it("lets operators reorder graph sources to control precedence", () => {
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        unobserve() {}
        disconnect() {}
      }
    );
    render(
      <CreateIndexDialog
        open
        onClose={() => undefined}
        tableName="docs"
        onIndexCreated={() => undefined}
        schema={null}
        artifactSourcesSupported
      />
    );

    fireEvent.click(screen.getByRole("radio", { name: "Graph" }));
    fireEvent.click(screen.getByRole("button", { name: "Add graph source" }));
    const artifacts = screen.getAllByPlaceholderText("relations_v1");
    fireEvent.change(artifacts[0], { target: { value: "primary_relations" } });
    fireEvent.change(artifacts[1], { target: { value: "fallback_relations" } });
    fireEvent.click(screen.getByRole("button", { name: "Move graph source 2 earlier" }));

    expect(
      screen.getAllByPlaceholderText("relations_v1").map((input) => input.getAttribute("value"))
    ).toEqual(["fallback_relations", "primary_relations"]);
  });

  it("uses direct graph fields and hides artifact controls when unsupported", () => {
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        unobserve() {}
        disconnect() {}
      }
    );
    render(
      <CreateIndexDialog
        open
        onClose={() => undefined}
        tableName="docs"
        onIndexCreated={() => undefined}
        schema={null}
        artifactSourcesSupported={false}
      />
    );

    expect(screen.getByTestId("index-form").getAttribute("data-artifact-sources")).toBe("false");
    fireEvent.click(screen.getByRole("radio", { name: "Graph" }));

    expect(screen.getByText(/supports graph indexes over document fields/i)).toBeTruthy();
    expect(screen.getByPlaceholderText("related")).toBeTruthy();
    expect(screen.queryByPlaceholderText("relations_v1")).toBeNull();
    expect(screen.queryByRole("radio", { name: "Artifact streams" })).toBeNull();
  });
});
