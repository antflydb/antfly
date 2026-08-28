import { type IndexConfig, isValidGraphMaterializedSourceTemplate } from "@antfly/sdk";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TableSchema } from "../api";
import CreateIndexDialog, {
  buildGraphEdgeTypeConfig,
  buildGraphSourceConfig,
  getSchemaFieldNames,
} from "./CreateIndexDialog";
import { parseAdvancedIndexConfig, usesArtifactBackedIndexSource } from "./create-index-config";

const mocks = vi.hoisted(() => ({
  createIndex: vi.fn(),
}));

vi.mock("../hooks/use-api-config", () => ({
  useApi: () => ({ indexes: { create: mocks.createIndex } }),
}));

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
  beforeEach(() => {
    mocks.createIndex.mockReset();
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        unobserve() {}
        disconnect() {}
      }
    );
  });

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

  it("validates graph materialized source templates with the server grammar", () => {
    expect(isValidGraphMaterializedSourceTemplate("{{ _doc.key }}")).toBe(true);
    expect(isValidGraphMaterializedSourceTemplate("{{ _artifact.value.owner.id }}")).toBe(false);
    expect(
      isValidGraphMaterializedSourceTemplate("{{ _artifact.value.source }}:{{ _doc.key }}")
    ).toBe(false);
    expect(isValidGraphMaterializedSourceTemplate("{{ _artifact.value.owner-id }}")).toBe(false);
    expect(isValidGraphMaterializedSourceTemplate("{{ _artifact.value.owner. }}")).toBe(false);
  });

  it("recognizes every supported artifact-backed request form", () => {
    const artifactBacked = [
      { name: "text", type: "full_text", sources: [{ artifact: "chunks" }] },
      { name: "text", type: "full_text", artifact_name: "chunks" },
      { name: "dense", type: "embeddings", dimension: 3, embedding_name: "dense_v1" },
      {
        name: "dense",
        type: "embeddings",
        dimension: 3,
        source_artifact_name: "chunks_v1",
      },
      { name: "graph", type: "graph", source: { artifact: "relations" } },
    ] satisfies IndexConfig[];
    for (const config of artifactBacked) {
      expect(usesArtifactBackedIndexSource(config)).toBe(true);
    }

    expect(usesArtifactBackedIndexSource({ name: "text", type: "full_text", field: "body" })).toBe(
      false
    );
    expect(
      usesArtifactBackedIndexSource({
        name: "graph",
        type: "graph",
        edge_types: [{ name: "related", field: "related_ids" }],
      })
    ).toBe(false);
  });

  it("uses the runtime-configured client for index creation", async () => {
    render(
      <CreateIndexDialog
        open
        onClose={() => undefined}
        tableName="docs"
        onIndexCreated={() => undefined}
        schema={null}
      />
    );

    fireEvent.click(screen.getByRole("switch"));
    fireEvent.change(screen.getByLabelText("Advanced index JSON"), {
      target: { value: '{"name":"text","type":"full_text","field":"body"}' },
    });
    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => expect(mocks.createIndex).toHaveBeenCalledOnce());
    expect(mocks.createIndex).toHaveBeenCalledWith("docs", "text", {
      type: "full_text",
      field: "body",
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
    expect(
      parseAdvancedIndexConfig(
        JSON.stringify({
          name: "artifact_text",
          type: "full_text",
          sources: [{ artifact: "document_chunks_v1", field: "summary" }],
        })
      )
    ).toMatchObject({
      name: "artifact_text",
      type: "full_text",
      sources: [{ artifact: "document_chunks_v1", field: "summary" }],
    });
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
          name: "blank_source_field",
          type: "full_text",
          sources: [{ artifact: "chunks_v1", field: " " }],
        })
      )
    ).toThrow("field must be a non-empty string");
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
          name: "orphaned_embedding_source",
          type: "embeddings",
          source_artifact_name: "document_chunks_v1",
        })
      )
    ).toThrow("requires a non-empty embedding_name");
    expect(() =>
      parseAdvancedIndexConfig(
        JSON.stringify({
          name: "empty_embedding_artifact",
          type: "embeddings",
          embedding_name: " ",
        })
      )
    ).toThrow("embedding_name must be a non-empty string");
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

  it("describes the index kind selected in Raw JSON", () => {
    render(
      <CreateIndexDialog
        open
        onClose={() => undefined}
        tableName="docs"
        onIndexCreated={() => undefined}
        schema={null}
      />
    );

    fireEvent.click(screen.getByRole("switch"));
    fireEvent.change(screen.getByLabelText("Advanced index JSON"), {
      target: { value: '{"name":"relations","type":"graph","edge_types":[]}' },
    });

    expect(screen.getByText("Create a new graph index for your table.")).toBeTruthy();
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

  it("preserves artifact-source drafts while a rolling upgrade is pending", () => {
    const commonProps = {
      open: true,
      onClose: () => undefined,
      tableName: "docs",
      onIndexCreated: () => undefined,
      schema: null,
    };
    const { rerender } = render(
      <CreateIndexDialog
        {...commonProps}
        artifactSourcesSupported
        artifactSourcesState="available"
      />
    );

    fireEvent.click(screen.getByRole("radio", { name: "Full-text" }));
    fireEvent.click(screen.getByRole("radio", { name: "Artifact streams" }));
    fireEvent.change(screen.getByPlaceholderText("document_chunks_v1"), {
      target: { value: "document_units_v1" },
    });

    rerender(
      <CreateIndexDialog
        {...commonProps}
        artifactSourcesSupported={false}
        artifactSourcesState="upgrade_pending"
      />
    );

    expect(screen.getByPlaceholderText("document_chunks_v1").getAttribute("value")).toBe(
      "document_units_v1"
    );
    expect(screen.getByText(/drafts remain editable/i)).toBeTruthy();
    expect(screen.getByRole("radio", { name: "Artifact streams" })).toBeTruthy();
  });

  it("keeps capability discovery failures distinct from unsupported deployments", async () => {
    const retry = vi.fn();
    mocks.createIndex.mockResolvedValue(undefined);
    render(
      <CreateIndexDialog
        open
        onClose={() => undefined}
        tableName="docs"
        onIndexCreated={() => undefined}
        schema={null}
        artifactSourcesSupported={undefined}
        artifactSourcesCapabilityError
        onRetryArtifactSourcesCapability={retry}
      />
    );

    expect(screen.getByText(/could not verify artifact-source support/i)).toBeTruthy();
    expect(screen.queryByText(/unavailable on this deployment/i)).toBeNull();
    fireEvent.click(screen.getByRole("radio", { name: "Graph" }));
    expect(screen.queryByText(/artifact-backed graph sources are unavailable/i)).toBeNull();
    expect(screen.getByPlaceholderText("related")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(retry).toHaveBeenCalledOnce();

    fireEvent.click(screen.getAllByRole("switch")[0]);
    fireEvent.change(screen.getByLabelText("Advanced index JSON"), {
      target: {
        value:
          '{"name":"document_text","type":"full_text","sources":[{"artifact":"document_units_v1"}]}',
      },
    });
    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => expect(mocks.createIndex).toHaveBeenCalledOnce());
  });

  it("does not report artifact-backed graph sources as unsupported while capability discovery loads", () => {
    render(
      <CreateIndexDialog
        open
        onClose={() => undefined}
        tableName="docs"
        onIndexCreated={() => undefined}
        schema={null}
        artifactSourcesSupported={undefined}
      />
    );

    expect(
      screen.getByText(/checking whether this deployment supports artifact-backed indexes/i)
    ).toBeTruthy();
    fireEvent.click(screen.getByRole("radio", { name: "Graph" }));
    expect(screen.queryByText(/artifact-backed graph sources are unavailable/i)).toBeNull();
    expect(screen.getByPlaceholderText("related")).toBeTruthy();
  });
});
