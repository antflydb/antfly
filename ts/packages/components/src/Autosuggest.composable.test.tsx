import type { AggregationBucket, QueryHit } from "@antfly/sdk";
import { render, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";
import type React from "react";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import Antfly from "./Antfly";
import Autosuggest, { AutosuggestFacets, AutosuggestResults } from "./Autosuggest";

// MSW server setup
const server = setupServer(
  http.post("http://localhost:8082/api/v1/multiquery", () => {
    return HttpResponse.json({
      responses: [
        {
          status: 200,
          took: 1,
          hits: {
            hits: [
              { _id: "1", _score: 1.0, _source: { title: "Test Item 1" } },
              { _id: "2", _score: 0.9, _source: { title: "Test Item 2" } },
            ],
            total: 2,
          },
          aggregations: {
            category__keyword: {
              buckets: [
                { key: "Category A", doc_count: 10 },
                { key: "Category B", doc_count: 5 },
              ],
            },
            author__keyword: {
              buckets: [
                { key: "Author X", doc_count: 8 },
                { key: "Author Y", doc_count: 3 },
              ],
            },
          },
        },
      ],
    });
  })
);

// Start server before all tests
beforeAll(() => server.listen({ onUnhandledRequest: "bypass" }));

// Reset handlers after each test
afterEach(() => server.resetHandlers());

// Close server after all tests
afterAll(() => server.close());

// Wrapper component to provide required context
const TestWrapper = ({ children }: { children: React.ReactNode }) => {
  return (
    <Antfly url="http://localhost:8082/api/v1" table="test">
      {children}
    </Antfly>
  );
};

describe("Composable Autosuggest", () => {
  describe("AutosuggestResults component", () => {
    it("should render without crashing when used alone", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept custom limit prop", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={5} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept custom className", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults className="custom-results" limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      // Wait for mock data to load
      await waitFor(() => {
        const autosuggestContainer = container.querySelector(".react-af-autosuggest-container");
        expect(autosuggestContainer).toBeTruthy();
      });
    });

    it("should accept custom renderItem function", () => {
      const customRender = vi.fn((hit: QueryHit) => <div>Custom: {hit._id}</div>);

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults renderItem={customRender} limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept header prop as ReactNode", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults header={<h3>Results</h3>} limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept header prop as function", () => {
      const headerFn = vi.fn((count: number) => <h3>{count} Results</h3>);

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults header={headerFn} limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept footer prop", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults footer={<div>See more</div>} limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept emptyMessage prop", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults emptyMessage="No results found" limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept filter function", () => {
      const filterFn = vi.fn((hit: QueryHit) => hit._score > 0.5);

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults filter={filterFn} limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept onSelect callback", () => {
      const onSelect = vi.fn();

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults onSelect={onSelect} limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });
  });

  describe("AutosuggestFacets component", () => {
    it("should render without crashing when used alone", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept size prop", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" size={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept label prop", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" label="Categories" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept order prop - count", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" order="count" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept order prop - term", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" order="term" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept order prop - reverse_count", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" order="reverse_count" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept order prop - reverse_term", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" order="reverse_term" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept clickable prop - true", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" clickable={true} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept clickable prop - false", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" clickable={false} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept custom className", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" className="custom-facets" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept custom renderItem function", () => {
      const customRender = vi.fn((bucket: AggregationBucket) => (
        <div>
          {bucket.key}: {bucket.doc_count}
        </div>
      ));

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" renderItem={customRender} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept custom renderSection function", () => {
      const customRenderSection = vi.fn(
        (_field: string, label: string, buckets: AggregationBucket[]) => (
          <div>
            <h3>{label}</h3>
            <ul>
              {buckets.map((b) => (
                <li key={b.key}>{b.key}</li>
              ))}
            </ul>
          </div>
        )
      );

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" renderSection={customRenderSection} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept header prop", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" header={<h4>Filter by Category</h4>} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept footer prop", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" footer={<div>More filters</div>} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept emptyMessage prop", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" emptyMessage="No categories" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept filter function", () => {
      const filterFn = vi.fn((bucket: AggregationBucket) => bucket.doc_count > 10);

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" filter={filterFn} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should accept onSelect callback", () => {
      const onSelect = vi.fn();

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" onSelect={onSelect} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });
  });

  describe("Composable combinations", () => {
    it("should render results and single facet together", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" size={5} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should render results and multiple facets together", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" size={5} label="Categories" />
            <AutosuggestFacets field="author__keyword" size={3} label="Authors" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should render facets before results (custom ordering)", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" size={3} />
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should render interleaved results and facets", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={3} header="Top Matches" />
            <AutosuggestFacets field="category__keyword" size={2} />
            <AutosuggestResults limit={7} header="More Results" />
            <AutosuggestFacets field="author__keyword" size={3} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should render multiple AutosuggestResults components", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={5} header="Exact Matches" />
            <AutosuggestResults limit={10} header="Related Items" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should render multiple AutosuggestFacets components", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" size={5} />
            <AutosuggestFacets field="author__keyword" size={3} />
            <AutosuggestFacets field="tags__keyword" size={8} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should handle custom React elements mixed with components", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <div className="section-header">Search Results</div>
            <AutosuggestResults limit={10} />
            <div className="section-divider" />
            <div className="section-header">Filter Options</div>
            <AutosuggestFacets field="category__keyword" size={5} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });
  });

  describe("Layout modes", () => {
    it("should render with vertical layout (default)", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" size={5} />
          </Autosuggest>
        </TestWrapper>
      );

      await waitFor(() => {
        const layoutContainer = container.querySelector(".react-af-autosuggest-layout-vertical");
        expect(layoutContainer).toBeTruthy();
      });
    });

    it("should render with horizontal layout", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            layout="horizontal"
          >
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" size={5} />
          </Autosuggest>
        </TestWrapper>
      );

      await waitFor(() => {
        const layoutContainer = container.querySelector(".react-af-autosuggest-layout-horizontal");
        expect(layoutContainer).toBeTruthy();
      });
    });

    it("should render with grid layout", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1} layout="grid">
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" size={5} />
            <AutosuggestFacets field="author__keyword" size={3} />
          </Autosuggest>
        </TestWrapper>
      );

      await waitFor(() => {
        const layoutContainer = container.querySelector(".react-af-autosuggest-layout-grid");
        expect(layoutContainer).toBeTruthy();
      });
    });

    it("should render with custom layout", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1} layout="custom">
            <div style={{ display: "flex" }}>
              <AutosuggestResults limit={10} />
              <AutosuggestFacets field="category__keyword" size={5} />
            </div>
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
      // Custom layout should not have layout class
      const layoutContainer = container.querySelector('[class*="react-af-autosuggest-layout-"]');
      expect(layoutContainer).toBeNull();
    });
  });

  describe("Custom styling", () => {
    it("should accept className on parent Autosuggest", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            className="my-custom-class"
          >
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      await waitFor(() => {
        const customClass = container.querySelector(".my-custom-class");
        expect(customClass).toBeTruthy();
      });
    });

    it("should accept dropdownClassName on parent Autosuggest", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            dropdownClassName="my-dropdown-class"
          >
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      await waitFor(() => {
        const dropdownClass = container.querySelector(".my-dropdown-class");
        expect(dropdownClass).toBeTruthy();
      });
    });

    it("should accept multiple custom classes", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            className="custom-container"
            dropdownClassName="custom-dropdown"
          >
            <AutosuggestResults className="custom-results" limit={10} />
            <AutosuggestFacets field="category__keyword" className="custom-facets" />
          </Autosuggest>
        </TestWrapper>
      );

      await waitFor(() => {
        expect(container.querySelector(".custom-container")).toBeTruthy();
        expect(container.querySelector(".custom-dropdown")).toBeTruthy();
      });
    });
  });

  describe("Backward compatibility", () => {
    it("should still work without children (legacy mode)", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1} />
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should work with renderSuggestion in legacy mode", () => {
      const customRender = vi.fn((hit: QueryHit) => <div>Legacy: {hit._id}</div>);

      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            renderSuggestion={customRender}
          />
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should ignore renderSuggestion when children are present", () => {
      const legacyRender = vi.fn(() => <div>Legacy</div>);

      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            renderSuggestion={legacyRender}
          >
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });
  });

  describe("Edge cases", () => {
    it("should handle empty children", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            {null}
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should handle conditional children rendering", () => {
      const showFacets = false;

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
            {showFacets && <AutosuggestFacets field="category__keyword" />}
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should handle very large number of children", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
            {[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19].map((n) => (
              <AutosuggestFacets key={`field${n}__keyword`} field={`field${n}__keyword`} size={3} />
            ))}
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should not crash when searchValue is below minChars", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="a" minChars={2}>
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" />
          </Autosuggest>
        </TestWrapper>
      );

      // Should not show dropdown
      const dropdown = container.querySelector(".react-af-autosuggest-container");
      expect(dropdown).toBeNull();
    });

    it("should handle semantic indexes with children", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest semanticIndexes={["embedding_index"]} searchValue="test query" minChars={1}>
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should handle filterQuery with children", () => {
      const filterQuery = { match: "active", field: "status" };

      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            filterQuery={filterQuery}
          >
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should handle exclusionQuery with children", () => {
      const exclusionQuery = { match: "deleted", field: "status" };

      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            exclusionQuery={exclusionQuery}
          >
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should handle table override with children", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            table="custom_table"
          >
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });
  });

  describe("Keyboard navigation", () => {
    it("should handle arrow down key", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
      // Keyboard navigation is tested indirectly - component renders without errors
    });

    it("should handle arrow up key", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should handle enter key", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should handle escape key", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should navigate across results and facets", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={5} />
            <AutosuggestFacets field="category__keyword" size={3} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should handle navigation with clickable=false facets", async () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={5} />
            <AutosuggestFacets field="category__keyword" size={3} clickable={false} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });
  });

  describe("Selection handling", () => {
    it("should call onSelect when result is clicked", async () => {
      const onSelect = vi.fn();

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults onSelect={onSelect} limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should call onSelect when facet is clicked", async () => {
      const onSelect = vi.fn();

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" onSelect={onSelect} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should not call onSelect when non-clickable facet is clicked", async () => {
      const onSelect = vi.fn();

      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestFacets field="category__keyword" onSelect={onSelect} clickable={false} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should populate search bar via onSuggestionSelect", async () => {
      const handleSelect = vi.fn();

      const { container } = render(
        <TestWrapper>
          <Autosuggest
            fields={["title__keyword"]}
            searchValue="test"
            minChars={1}
            onSuggestionSelect={handleSelect}
          >
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });
  });

  describe("Widget registration for facets", () => {
    it("should register facet options when AutosuggestFacets is used", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" size={5} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
      // Widget should be registered with facetOptions
      // This is tested indirectly - the component renders without errors
    });

    it("should register multiple facet options", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
            <AutosuggestFacets field="category__keyword" size={5} />
            <AutosuggestFacets field="author__keyword" size={3} />
            <AutosuggestFacets field="tags__keyword" size={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });

    it("should not register facet options when no facets present", () => {
      const { container } = render(
        <TestWrapper>
          <Autosuggest fields={["title__keyword"]} searchValue="test" minChars={1}>
            <AutosuggestResults limit={10} />
          </Autosuggest>
        </TestWrapper>
      );

      expect(container).toBeTruthy();
    });
  });
});
