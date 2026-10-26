import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TableSchema } from "../api";
import DocumentBuilder from "./DocumentBuilder";

const { batch } = vi.hoisted(() => ({ batch: vi.fn() }));

vi.mock("../api", () => ({ api: { tables: { batch } } }));

const dynamicSchema: TableSchema = {
  version: 0,
  default_type: "doc",
  enforce_types: false,
  document_schemas: {
    doc: {
      schema: {
        type: "object",
        additionalProperties: true,
        "x-antfly-dynamic-indexing": { mode: "infer_types" },
      },
    },
  },
};

const declaredSchema: TableSchema = {
  default_type: "article",
  document_schemas: {
    article: {
      schema: {
        type: "object",
        properties: { title: { type: "string" }, views: { type: "integer" } },
      },
    },
  },
};

function enterDocument(json: string, id = "doc-1") {
  fireEvent.change(screen.getByLabelText("Document ID *"), { target: { value: id } });
  fireEvent.change(screen.getByLabelText("Document JSON"), { target: { value: json } });
}

function insertDocument() {
  fireEvent.click(screen.getByRole("button", { name: "Insert Document" }));
}

describe("DocumentBuilder", () => {
  beforeEach(() => {
    batch.mockReset().mockResolvedValue({});
    vi.spyOn(console, "error").mockImplementation(() => undefined);
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it.each([undefined, {}])("inserts JSON when schema properties are %j", async (properties) => {
    render(
      <DocumentBuilder
        tableName="docs"
        schema={{
          ...dynamicSchema,
          document_schemas: {
            doc: {
              schema: { ...dynamicSchema.document_schemas?.doc.schema, type: "object", properties },
            },
          },
        }}
      />
    );
    const document = {
      title: "First document",
      count: 3,
      active: true,
      tags: ["demo"],
      nested: { score: 2 },
    };
    enterDocument(JSON.stringify(document));
    insertDocument();

    await waitFor(() =>
      expect(batch).toHaveBeenCalledWith("docs", { inserts: { "doc-1": document } })
    );
    expect(await screen.findByText("Successfully inserted document.")).toBeTruthy();
    expect((screen.getByLabelText("Document ID *") as HTMLInputElement).value).toBe("");
    expect((screen.getByLabelText("Document JSON") as HTMLTextAreaElement).value).toBe("{}");
  });

  it.each([
    "{",
    "",
    "null",
    "[]",
    "42",
    '"text"',
    "true",
  ])("rejects non-object or invalid JSON %j without submitting", async (json) => {
    render(<DocumentBuilder tableName="docs" schema={dynamicSchema} />);
    enterDocument(json);
    insertDocument();

    expect(await screen.findByText(/Enter a valid JSON object/)).toBeTruthy();
    expect(batch).not.toHaveBeenCalled();
    expect((screen.getByLabelText("Document JSON") as HTMLTextAreaElement).value).toBe(json);
  });

  it("requires a document ID for JSON entry", async () => {
    render(<DocumentBuilder tableName="docs" schema={dynamicSchema} />);
    enterDocument('{"title":"Draft"}', " ");
    insertDocument();

    expect(await screen.findByText(/Document ID is required/)).toBeTruthy();
    expect(batch).not.toHaveBeenCalled();
  });

  it("preserves JSON and ID when insertion fails", async () => {
    batch.mockRejectedValue(new Error("Service unavailable"));
    render(<DocumentBuilder tableName="docs" schema={dynamicSchema} />);
    const json = '{"title":"Draft"}';
    enterDocument(json);
    insertDocument();

    expect(await screen.findByText("Failed to insert document: Service unavailable")).toBeTruthy();
    expect((screen.getByLabelText("Document ID *") as HTMLInputElement).value).toBe("doc-1");
    expect((screen.getByLabelText("Document JSON") as HTMLTextAreaElement).value).toBe(json);
  });

  it("keeps the field form and defaults for declared properties", async () => {
    render(<DocumentBuilder tableName="docs" schema={declaredSchema} />);
    expect(screen.queryByLabelText("Document JSON")).toBeNull();
    fireEvent.change(screen.getByLabelText("Document ID *"), { target: { value: "article-1" } });
    fireEvent.change(screen.getByLabelText("title"), { target: { value: "Hello" } });
    insertDocument();

    await waitFor(() =>
      expect(batch).toHaveBeenCalledWith("docs", {
        inserts: { "article-1": { title: "Hello", views: 0 } },
      })
    );
    expect(await screen.findByText("Successfully inserted document.")).toBeTruthy();
    expect((screen.getByLabelText("title") as HTMLInputElement).value).toBe("");
  });

  it("resets the document when switching between declared and dynamic types", () => {
    render(
      <DocumentBuilder
        tableName="docs"
        schema={{
          ...declaredSchema,
          document_schemas: {
            ...declaredSchema.document_schemas,
            ...dynamicSchema.document_schemas,
          },
        }}
      />
    );
    fireEvent.change(screen.getByLabelText("Document ID *"), { target: { value: "article-1" } });
    fireEvent.change(screen.getByLabelText("title"), { target: { value: "Draft" } });
    fireEvent.change(screen.getByLabelText("Document Type"), { target: { value: "doc" } });
    expect((screen.getByLabelText("Document ID *") as HTMLInputElement).value).toBe("");
    expect((screen.getByLabelText("Document JSON") as HTMLTextAreaElement).value).toBe("{}");

    enterDocument('{"title":"Dynamic draft"}');
    fireEvent.change(screen.getByLabelText("Document Type"), { target: { value: "article" } });
    expect((screen.getByLabelText("Document ID *") as HTMLInputElement).value).toBe("");
    expect((screen.getByLabelText("title") as HTMLInputElement).value).toBe("");
  });
});
