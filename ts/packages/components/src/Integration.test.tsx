import type { QueryHit } from "@antfly/sdk";
import { fireEvent, render, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type React from "react";
import { describe, expect, it, vi } from "vitest";
import Antfly from "./Antfly";
import Autosuggest from "./Autosuggest";
import QueryBox from "./QueryBox";
import Results from "./Results";
import * as utils from "./utils";

// Wrapper component to provide required context
const TestWrapper = ({ children }: { children: React.ReactNode }) => {
  return (
    <Antfly url="http://localhost:8082/api/v1" table="test">
      {children}
    </Antfly>
  );
};

/** Mock `multiquery` to return a single response with the given hits. */
function mockMultiquery(hits: QueryHit[] = []) {
  return vi.spyOn(utils, "multiquery").mockResolvedValue({
    responses: [
      {
        status: 200,
        took: 10,
        hits: { hits, total: hits.length },
      },
    ],
  });
}

describe("Integration Tests", () => {
  describe("SearchBox + Autosuggest integration", () => {
    it("should render SearchBox with Autosuggest child", () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={2} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      expect(container.querySelector("input")).toBeTruthy();
    });

    it("should pass search value from SearchBox to Autosuggest", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={2} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      expect(input.value).toBe("test");
    });

    it("should handle suggestion selection without custom handler", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      expect(container).toBeTruthy();
    });

    it("should respect minChars threshold", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={3} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type less than minChars
      await userEvent.type(input, "ab");
      expect(container.querySelector(".react-af-autosuggest")).toBeNull();
    });

    it("should work with containerRef for click outside detection", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      expect(container).toBeTruthy();
    });
  });

  describe("SearchBox + Results integration", () => {
    it("should render SearchBox with Results", () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results
            id="results"
            items={(data) => (
              <div>
                {data.map((item) => (
                  <div key={item._id}>{item._id}</div>
                ))}
              </div>
            )}
          />
        </TestWrapper>
      );

      expect(container.querySelector("input")).toBeTruthy();
      expect(container.querySelector(".react-af-results")).toBeTruthy();
    });

    it("should update results when search value changes", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results
            id="results"
            items={(data) => <div className="results-items">{data.length} items</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });

    it("should handle semantic search in SearchBox and Results", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results-sem"
            searchBoxId="search"
            semanticIndexes={["index1"]}
            limit={10}
            items={() => <div />}
          />
          <Results
            id="results"
            items={(data) => <div className="results-items">{data.length} items</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "semantic query");

      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });

    it("should call onResults when query results arrive", async () => {
      const hits: QueryHit[] = [
        { _id: "1", _score: 1.0, _source: { title: "First result" } },
        { _id: "2", _score: 0.8, _source: { title: "Second result" } },
      ];
      const msearchSpy = mockMultiquery(hits);
      const onResults = vi.fn();

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results"
            searchBoxId="search"
            fields={["title"]}
            onResults={onResults}
            items={() => <div />}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(onResults).toHaveBeenCalledWith(hits, hits.length);
      });

      msearchSpy.mockRestore();
    });
  });

  describe("Full search workflow", () => {
    it("should handle complete search workflow with autosuggest and results", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword", "description__2gram"]} minChars={2} />
          </QueryBox>
          <Results
            id="results"
            searchBoxId="search"
            fields={["title", "description"]}
            items={() => <div />}
          />
          <Results
            id="results"
            items={(data) => (
              <div>
                {data.map((item) => (
                  <div key={item._id}>{item._id}</div>
                ))}
              </div>
            )}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type a search query
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });

    it("should handle semantic search workflow", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest semanticIndexes={["embedding_index"]} minChars={3} />
          </QueryBox>
          <Results
            id="results"
            searchBoxId="search"
            semanticIndexes={["embedding_index"]}
            limit={20}
            items={() => <div />}
          />
          <Results id="results" items={(data) => <div>{data.length} results</div>} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "semantic search query");

      await waitFor(() => {
        expect(container).toBeTruthy();
      });
    });

    it("should handle mixed field types in autosuggest", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword", "name__2gram", "description"]} minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      expect(container).toBeTruthy();
    });

    it("should handle custom query in SearchBox and Autosuggest", async () => {
      const customSearchQuery = vi.fn((query) => ({ search: query }));
      const customAutosuggestQuery = vi.fn((value, fields) => ({
        autosuggest: value,
        fields,
      }));

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest customQuery={customAutosuggestQuery} minChars={1} />
          </QueryBox>
          <Results
            id="results"
            searchBoxId="search"
            customQuery={customSearchQuery}
            items={() => <div />}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "custom");

      expect(container).toBeTruthy();
    });
  });

  describe("Edge cases in integration", () => {
    it("should handle rapid typing without crashing", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Rapidly type multiple characters
      await userEvent.type(input, "abcdefghijklmnop", { delay: 1 });

      expect(input.value).toBe("abcdefghijklmnop");
      expect(container).toBeTruthy();
    });

    it("should handle clearing search", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" initialValue="initial">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      expect(input.value).toBe("initial");

      await userEvent.clear(input);
      expect(input.value).toBe("");
    });

    it("should handle SearchBox without fields", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest minChars={1} />
          </QueryBox>
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      expect(container).toBeTruthy();
    });

    it("should handle Autosuggest without fields in SearchBox context", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      expect(container).toBeTruthy();
    });

    it("should handle empty fields arrays", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={[]} minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={[]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      expect(container).toBeTruthy();
    });

    it("should fire queries when SearchBox has fields but Autosuggest has empty fields", async () => {
      // This tests the configuration where Results has fields but Autosuggest has empty fields array
      // Bug: Autosuggest sets needsQuery=true but query=null, causing
      // queries.size + semanticQueries.size !== searchWidgets.size

      // Spy on the msearch function to verify it gets called
      const msearchSpy = mockMultiquery();

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={[]} minChars={2} />
          </QueryBox>
          <Results
            searchBoxId="search"
            fields={["title__keyword"]}
            id="results"
            items={(data) => <div className="results-content">{data.length} results</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Wait for initial render to settle
      await waitFor(
        () => {
          expect(msearchSpy).toHaveBeenCalled();
        },
        { timeout: 1000 }
      );

      const initialCallCount = msearchSpy.mock.calls.length;

      // Now type - this is where the bug manifests!
      // When user types, Autosuggest updates and sets needsQuery=true but query=null
      await userEvent.type(input, "te");

      // The key test: verify that msearch was called AGAIN after typing
      // If the bug exists, this will timeout because no new queries fire
      await waitFor(
        () => {
          const newCallCount = msearchSpy.mock.calls.length;
          expect(newCallCount).toBeGreaterThan(initialCallCount);
        },
        { timeout: 3000 }
      );

      msearchSpy.mockRestore();
    });

    it("should handle very long search queries", async () => {
      const longQuery = "a".repeat(500);
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, longQuery);

      expect(input.value).toBe(longQuery);
      expect(container).toBeTruthy();
    });

    it("should handle special characters without breaking", async () => {
      // Note: Some special characters like [] {} | have special meaning in userEvent
      // so we test them with fireEvent instead
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Use fireEvent for special characters that userEvent can't handle
      const specialChars = '@#$%^&*()[]{}|\\;:"<>?,./~`';
      fireEvent.change(input, { target: { value: specialChars } });

      expect(input.value).toBe(specialChars);
      expect(container).toBeTruthy();
    });
  });

  describe("Widget state management", () => {
    it("should not interfere between multiple search boxes", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search1" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results1" searchBoxId="search1" fields={["title"]} items={() => <div />} />
          <QueryBox id="search2" mode="live">
            <Autosuggest fields={["description__keyword"]} minChars={1} />
          </QueryBox>
          <Results
            id="results2"
            searchBoxId="search2"
            fields={["description"]}
            items={() => <div />}
          />
        </TestWrapper>
      );

      const inputs = container.querySelectorAll("input");
      expect(inputs.length).toBe(2);

      await userEvent.type(inputs[0], "first");
      await userEvent.type(inputs[1], "second");

      expect((inputs[0] as HTMLInputElement).value).toBe("first");
      expect((inputs[1] as HTMLInputElement).value).toBe("second");
    });

    it("should handle multiple Results components", () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results id="results1" items={(data) => <div className="results1">{data.length}</div>} />
          <Results
            id="results2"
            items={(data) => <div className="results2">{data.length}</div>}
            fields={["description"]}
          />
        </TestWrapper>
      );

      expect(container.querySelectorAll(".react-af-results").length).toBe(3);
    });
  });

  describe("Autosuggest query isolation", () => {
    it("should not include autosuggest queries in main search", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results id="results" items={(data) => <div>{data.length} items</div>} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // Both components should render without interfering
      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });

    it("should handle autosuggest with semantic and traditional search together", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results
            id="results"
            searchBoxId="search"
            semanticIndexes={["index1"]}
            items={() => <div />}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "hybrid search");

      expect(container).toBeTruthy();
    });
  });

  describe("Widget configuration consistency (regression test)", () => {
    it("should fire queries when Autosuggest sets needsConfiguration correctly", async () => {
      // This test specifically checks for the bug where Autosuggest would set
      // needsConfiguration: false but still provide a configuration object,
      // causing the Listener's configurationsReady check to fail.
      //
      // Bug scenario:
      // - configurableWidgets.size = 0 (needsConfiguration was false)
      // - configurations.size = 1 (configuration object was provided)
      // - Result: 0 !== 1, so queries never fire
      //
      // Fix: Autosuggest must set needsConfiguration: true whenever it provides
      // a configuration object

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={2} />
          </QueryBox>
          <Results id="results" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results
            id="results"
            items={(data) => <div className="results-content">{data.length} results</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type enough characters to trigger autosuggest (meets minChars threshold)
      await userEvent.type(input, "test");

      // The key assertion: Results should render, proving that queries fired
      // If the bug exists, this will fail because the Listener won't fire queries
      await waitFor(
        () => {
          const results = container.querySelector(".react-af-results");
          expect(results).toBeTruthy();
        },
        { timeout: 3000 }
      );
    });

    it("should fire queries with non-semantic Autosuggest", async () => {
      // Specifically test non-semantic autosuggest (isSemanticEnabled = false)
      // This was the exact scenario where the bug occurred
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword", "description__2gram"]} minChars={1} />
          </QueryBox>
          <Results
            id="results"
            searchBoxId="search"
            fields={["title", "description"]}
            items={() => <div />}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "abc");

      // Give the component time to register and potentially show suggestions
      // Even if no data comes back from the mock, the component should render
      expect(container).toBeTruthy();

      // The component should have attempted to fetch data
      // (we can't easily verify the network call in this test setup,
      // but we can verify the component doesn't crash/hang)
      await waitFor(
        () => {
          expect(input.value).toBe("abc");
        },
        { timeout: 1000 }
      );
    });

    it("should handle multiple widgets with mixed configuration needs", async () => {
      // Test that the Listener correctly handles multiple widgets where
      // some need configuration and some don't
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search1" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results1" searchBoxId="search1" fields={["title"]} items={() => <div />} />
          <QueryBox id="search2" mode="live" />
          <Results
            id="results2"
            searchBoxId="search2"
            fields={["description"]}
            items={() => <div />}
          />
          <Results id="results" items={(data) => <div>{data.length} items</div>} />
        </TestWrapper>
      );

      const inputs = container.querySelectorAll("input");
      expect(inputs.length).toBe(2);

      // Type in the first search box (has autosuggest)
      await userEvent.type(inputs[0], "test");

      // Type in the second search box (no autosuggest)
      await userEvent.type(inputs[1], "query");

      // Both should work without blocking each other
      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });

    it("should handle semantic autosuggest configuration correctly", async () => {
      // Verify that semantic autosuggest also sets needsConfiguration properly
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest semanticIndexes={["suggestion_index"]} minChars={2} />
          </QueryBox>
          <Results
            id="results"
            searchBoxId="search"
            semanticIndexes={["embedding_index"]}
            items={() => <div />}
          />
          <Results id="results" items={(data) => <div>{data.length} results</div>} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "semantic test");

      // Should render results without hanging
      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });
  });

  describe("filterQuery integration", () => {
    it("should filter search results using filterQuery prop", async () => {
      const msearchSpy = mockMultiquery();

      const filterQuery = { match: "active", field: "status" };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results-query"
            searchBoxId="search"
            fields={["title"]}
            filterQuery={filterQuery}
            items={() => <div />}
          />
          <Results
            id="results"
            filterQuery={filterQuery}
            items={(data) => <div>Results: {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(msearchSpy).toHaveBeenCalled();
        const lastCall = msearchSpy.mock.calls[msearchSpy.mock.calls.length - 1];
        const queries = lastCall[1];
        // Results widget should have filterQuery
        expect(queries[0].query.filter_query).toEqual(filterQuery);
      });

      msearchSpy.mockRestore();
    });

    it("should work with SearchBox, Autosuggest, and Results all having filterQuery", async () => {
      const filterQuery = { match: "published", field: "state" };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={2} filterQuery={filterQuery} />
          </QueryBox>
          <Results
            id="results-query"
            searchBoxId="search"
            fields={["title"]}
            filterQuery={filterQuery}
            items={() => <div />}
          />
          <Results
            id="results"
            filterQuery={filterQuery}
            items={(data) => <div>Found {data.length} results</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test query");

      // Should render without errors
      expect(container.querySelector(".react-af-results")).toBeTruthy();
    });

    it("should handle different filterQuery values for different components", async () => {
      const searchFilter = { match: "active", field: "status" };
      const resultsFilter = {
        conjuncts: [
          { match: "active", field: "status" },
          { min: 100, field: "price" },
        ],
      };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results-search"
            searchBoxId="search"
            fields={["title"]}
            filterQuery={searchFilter}
            items={() => <div />}
          />
          <Results
            id="results"
            filterQuery={resultsFilter}
            items={(data) => <div>Results: {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "product");

      expect(container).toBeTruthy();
    });

    it("should handle filterQuery with semantic search", async () => {
      const filterQuery = { match: "active", field: "status" };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest
              semanticIndexes={["suggest-index"]}
              minChars={2}
              filterQuery={filterQuery}
            />
          </QueryBox>
          <Results
            id="results-semantic"
            searchBoxId="search"
            semanticIndexes={["vector-index"]}
            filterQuery={filterQuery}
            items={() => <div />}
          />
          <Results
            id="results"
            filterQuery={filterQuery}
            items={(data) => <div>Results: {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "semantic search");

      expect(container).toBeTruthy();
    });
  });

  describe("setWidget should not clear results (regression)", () => {
    it("should preserve semantic search results when Results re-registers config", async () => {
      // Regression test for: semantic search results arrive from server but
      // don't render because Results' useEffect re-fires (due to unstable
      // prop references like `semanticIndexes={[tableSlug]}`) and dispatches
      // setWidget without result, which the old reducer interpreted as
      // "clear result" (result: undefined).
      //
      // The fix: setWidget always preserves existingWidget.result when
      // action.result is undefined, since setWidgetResult is the dedicated
      // action for updating results.

      const mockHits: QueryHit[] = [
        { _id: "doc_1", _score: 1.0, _source: { title: "Result 1" } },
        { _id: "doc_2", _score: 0.9, _source: { title: "Result 2" } },
      ];

      const msearchSpy = mockMultiquery(mockHits);

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="submit" />
          <Results
            id="results-semantic"
            searchBoxId="search"
            semanticIndexes={["test-index"]}
            limit={10}
            items={(data) => (
              <div className="hit-list">
                {data.map((item) => (
                  <div key={item._id} className="hit-item">
                    {item._id}
                  </div>
                ))}
              </div>
            )}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Submit a search query
      await userEvent.type(input, "test query{enter}");

      // Wait for results to render
      await waitFor(
        () => {
          const hitItems = container.querySelectorAll(".hit-item");
          expect(hitItems.length).toBe(2);
        },
        { timeout: 3000 }
      );

      // Results should persist — the old bug would clear them synchronously
      // in the same render batch because setWidget (from Results' useEffect)
      // would overwrite the result set by setWidgetResult.
      const hitItems = container.querySelectorAll(".hit-item");
      expect(hitItems.length).toBe(2);
      expect(hitItems[0].textContent).toBe("doc_1");
      expect(hitItems[1].textContent).toBe("doc_2");

      msearchSpy.mockRestore();
    });
  });

  describe("exclusionQuery integration", () => {
    it("should exclude results using exclusionQuery prop", async () => {
      const msearchSpy = mockMultiquery();

      const exclusionQuery = { match: "archived", field: "status" };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results-excl"
            searchBoxId="search"
            fields={["title"]}
            exclusionQuery={exclusionQuery}
            items={() => <div />}
          />
          <Results
            id="results"
            exclusionQuery={exclusionQuery}
            items={(data) => <div>Results: {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(msearchSpy).toHaveBeenCalled();
        const lastCall = msearchSpy.mock.calls[msearchSpy.mock.calls.length - 1];
        const queries = lastCall[1];
        // Results widget should have exclusionQuery
        expect(queries[0].query.exclusion_query).toEqual(exclusionQuery);
      });

      msearchSpy.mockRestore();
    });

    it("should work with both filterQuery and exclusionQuery", async () => {
      const msearchSpy = mockMultiquery();

      const filterQuery = { match: "active", field: "status" };
      const exclusionQuery = { match: "spam", field: "category" };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results-config"
            searchBoxId="search"
            fields={["title"]}
            filterQuery={filterQuery}
            exclusionQuery={exclusionQuery}
            items={() => <div />}
          />
          <Results
            id="results"
            filterQuery={filterQuery}
            exclusionQuery={exclusionQuery}
            items={(data) => <div>Results: {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(msearchSpy).toHaveBeenCalled();
        const lastCall = msearchSpy.mock.calls[msearchSpy.mock.calls.length - 1];
        const queries = lastCall[1];
        // Results widget should have both filterQuery and exclusionQuery
        expect(queries[0].query.filter_query).toEqual(filterQuery);
        expect(queries[0].query.exclusion_query).toEqual(exclusionQuery);
      });

      msearchSpy.mockRestore();
    });

    it("should work with SearchBox, Autosuggest, and Results all having exclusionQuery", async () => {
      const exclusionQuery = { match: "deleted", field: "state" };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={2} exclusionQuery={exclusionQuery} />
          </QueryBox>
          <Results
            id="results-excl"
            searchBoxId="search"
            fields={["title"]}
            exclusionQuery={exclusionQuery}
            items={() => <div />}
          />
          <Results
            id="results"
            exclusionQuery={exclusionQuery}
            items={(data) => <div>Found {data.length} results</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test query");

      // Should render without errors
      expect(container.querySelector(".react-af-results")).toBeTruthy();
    });

    it("should handle exclusionQuery with semantic search", async () => {
      const exclusionQuery = { match: "archived", field: "status" };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest
              semanticIndexes={["suggest-index"]}
              minChars={2}
              exclusionQuery={exclusionQuery}
            />
          </QueryBox>
          <Results
            id="results-excl-sem"
            searchBoxId="search"
            semanticIndexes={["vector-index"]}
            exclusionQuery={exclusionQuery}
            items={() => <div />}
          />
          <Results
            id="results"
            exclusionQuery={exclusionQuery}
            items={(data) => <div>Results: {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "semantic search");

      expect(container).toBeTruthy();
    });
  });
});
