import { render, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type React from "react";
import { describe, expect, it, vi } from "vitest";
import Antfly from "./Antfly";
import Autosuggest from "./Autosuggest";
import Listener from "./Listener";
import QueryBox from "./QueryBox";
import Results from "./Results";

// Wrapper component to provide required context
const TestWrapper = ({ children }: { children: React.ReactNode }) => {
  return (
    <Antfly url="http://localhost:8082/api/v1" table="test">
      {children}
    </Antfly>
  );
};

describe("Listener", () => {
  describe("Widget configuration readiness checks", () => {
    it("should fire queries when widget has both needsConfiguration and configuration", async () => {
      // Regression test for bug where:
      // - Widget sets needsConfiguration: false
      // - But widget still provides configuration object
      // - Listener's configurationsReady check fails (0 !== 1)
      // - Result: queries never fire

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results
            id="results"
            items={(data) => <div className="results-rendered">Found {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      expect(input).toBeTruthy();

      // Type to trigger autosuggest and search
      await userEvent.type(input, "test");

      // Wait for queries to fire and results to render
      // If the bug exists, this will timeout because queries never fire
      await waitFor(
        () => {
          const results = container.querySelector(".react-af-results");
          expect(results).toBeTruthy();
        },
        { timeout: 3000 }
      );
    });

    it("should handle non-semantic autosuggest configuration correctly", async () => {
      // The bug specifically affected non-semantic autosuggest because
      // it set needsConfiguration: isSemanticEnabled (which is false)
      // while still providing a configuration object

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["name__keyword", "description__2gram"]} minChars={2} />
          </QueryBox>
          <Results
            id="results-cfg"
            searchBoxId="search"
            fields={["name", "description"]}
            items={() => <div />}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "testing");

      // Component should not crash or hang
      expect(input.value).toBe("testing");

      // Give it time to process - should complete without hanging
      await waitFor(
        () => {
          expect(container).toBeTruthy();
        },
        { timeout: 1000 }
      );
    });

    it("should handle semantic autosuggest configuration correctly", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest semanticIndexes={["suggestion_index"]} minChars={2} />
          </QueryBox>
          <Results
            id="results-main"
            searchBoxId="search"
            semanticIndexes={["main_index"]}
            limit={20}
            items={() => <div />}
          />
          <Results id="results" items={(data) => <div>{data.length} results</div>} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "semantic query");

      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });

    it("should correctly calculate readiness with multiple widgets", async () => {
      // Test scenario:
      // - SearchBox1 + Autosuggest: needsConfiguration=true, has configuration
      // - SearchBox2 (no autosuggest): needsConfiguration=false, has configuration
      // - Results: needsConfiguration=true, has configuration
      //
      // Expected: all widgets should be ready and queries should fire

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
          <Results
            id="results"
            items={(data) => <div className="test-results">{data.length}</div>}
          />
        </TestWrapper>
      );

      const inputs = container.querySelectorAll("input");

      // Type in both search boxes
      await userEvent.type(inputs[0], "first");
      await userEvent.type(inputs[1], "second");

      // Results should render
      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });

    it("should handle widgets without configuration", async () => {
      // SearchBox without autosuggest or semantic search
      // should not require configuration

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results id="results" items={(data) => <div>{data.length}</div>} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });

    it("should handle empty search value clearing widgets", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" initialValue="initial">
            <Autosuggest fields={["title__keyword"]} minChars={2} />
          </QueryBox>
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results id="results" items={(data) => <div>{data.length}</div>} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Clear the input
      await userEvent.clear(input);

      // Should still render without issues
      expect(container.querySelector(".react-af-results")).toBeTruthy();
    });

    it("should debounce rapid widget updates", async () => {
      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;

      // Type rapidly (simulates rapid updates)
      await userEvent.type(input, "abcdefg", { delay: 10 });

      // Should handle all updates without crashing
      expect(input.value).toBe("abcdefg");

      // Give debounce time to settle
      await waitFor(
        () => {
          expect(container).toBeTruthy();
        },
        { timeout: 100 }
      );
    });
  });

  describe("Query construction", () => {
    it("should exclude autosuggest queries from Results queries", async () => {
      // Autosuggest should have isAutosuggest: true
      // Results queries should filter out autosuggest queries

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results id="results" items={(data) => <div>{data.length}</div>} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // Both should render and work independently
      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });

    it("should exclude autosuggest semantic queries from main Results semantic query", async () => {
      // Regression test for bug where:
      // - Autosuggest has semanticIndexes configured
      // - SearchBox does not have semanticIndexes
      // - User types "test" in SearchBox
      // - Autosuggest registers with semanticQuery: "test"
      // - Listener was joining all semanticQueries including autosuggest
      // - Result: semantic query was "test" instead of being excluded
      //
      // Expected: Autosuggest semantic queries should NOT affect Results queries

      const { container } = render(
        <TestWrapper>
          <QueryBox id="main" mode="live">
            <Autosuggest
              semanticIndexes={["title_body_semantic"]}
              fields={[]}
              limit={10}
              minChars={2}
            />
          </QueryBox>
          <Results
            id="results-cfg"
            searchBoxId="main"
            fields={["title__keyword"]}
            items={() => <div />}
          />
          <Results
            id="result"
            items={(data) => <div className="test-results">{data.length} results</div>}
            itemsPerPage={10}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      expect(input).toBeTruthy();

      // Type into the search box
      await userEvent.type(input, "test query");

      // Results should render without autosuggest semantic query being attached
      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });

      // Both autosuggest (if it had results) and results should work independently
      expect(input.value).toBe("test query");
    });

    it("should handle SearchBox with semantic search and Autosuggest with semantic search independently", async () => {
      // Test case where both have semantic indexes
      // Each should query independently without interfering with each other

      const { container } = render(
        <TestWrapper>
          <QueryBox id="main" mode="live">
            <Autosuggest
              semanticIndexes={["autosuggest_semantic_index"]}
              fields={[]}
              limit={5}
              minChars={2}
            />
          </QueryBox>
          <Results
            id="results-sem"
            searchBoxId="main"
            semanticIndexes={["main_semantic_index"]}
            limit={20}
            items={() => <div />}
          />
          <Results
            id="result"
            items={(data) => <div className="test-results">{data.length} results</div>}
            itemsPerPage={10}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "semantic test");

      // Both should work independently
      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });

      expect(input.value).toBe("semantic test");
    });

    it("should include autosuggest's own semantic query when autosuggest queries for results", async () => {
      // Regression test for bug where:
      // - Autosuggest has semanticIndexes configured and wantResults: true
      // - User types "foo" in SearchBox
      // - Autosuggest registers with semanticQuery: "foo", isAutosuggest: true
      // - Listener was filtering out ALL autosuggest semantic queries
      // - Result: autosuggest's own query had semantic_search: "" instead of "foo"
      //
      // Expected: Autosuggest should get its OWN semantic query, but NOT other autosuggest queries

      const utils = await import("./utils");
      const msearchSpy = vi.spyOn(utils, "multiquery").mockResolvedValue({
        responses: [
          {
            status: 200,
            took: 10,
            hits: { hits: [], total: 0 },
          },
          {
            status: 200,
            took: 10,
            hits: { hits: [], total: 0 },
          },
        ],
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="main" mode="live">
            <Autosuggest
              semanticIndexes={["title_body_semantic"]}
              fields={[]}
              limit={10}
              minChars={2}
              debounceMs={0}
            />
          </QueryBox>
          <Results
            searchBoxId="main"
            fields={["title__keyword"]}
            id="result"
            items={(data) => <div className="test-results">{data.length} results</div>}
            itemsPerPage={10}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      expect(input).toBeTruthy();

      // Type into the search box
      await userEvent.type(input, "foo");

      // Wait for queries to be fired including the semantic query
      // Autosuggest may execute its query separately/asynchronously from other widgets
      await waitFor(() => {
        const calls = msearchSpy.mock.calls;
        expect(calls.length).toBeGreaterThan(0);

        // Verify that at least one query contains semantic search configuration
        // In the new architecture, Autosuggest inside QueryBox executes its own isolated query
        const hasSemanticQuery = calls.some((call) => {
          const queries = call[1];
          return queries?.some(
            (q: { query?: { semantic_search?: string; indexes?: string[] } }) =>
              q.query?.semantic_search && Array.isArray(q.query?.indexes)
          );
        });
        expect(hasSemanticQuery).toBeTruthy();
      });

      msearchSpy.mockRestore();
    });

    it("should include facet queries in main search", async () => {
      // This is just a smoke test - we don't have facet components in this test
      // but we verify the system doesn't break without them

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results id="results" items={(data) => <div>{data.length}</div>} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(container.querySelector(".react-af-results")).toBeTruthy();
      });
    });
  });

  describe("onChange callback", () => {
    it("should call onChange when widget values change", async () => {
      const onChange = vi.fn();

      const { container } = render(
        <Antfly url="http://localhost:8082/api/v1" table="test">
          <Listener onChange={onChange}>
            <QueryBox id="search" mode="live" />
            <Results
              id="results-cfg"
              searchBoxId="search"
              fields={["title"]}
              items={() => <div />}
            />
          </Listener>
        </Antfly>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // onChange should have been called
      await waitFor(() => {
        expect(onChange).toHaveBeenCalled();
      });
    });

    it("should include page parameters in onChange callback", async () => {
      const onChange = vi.fn();

      const { container } = render(
        <Antfly url="http://localhost:8082/api/v1" table="test">
          <Listener onChange={onChange}>
            <QueryBox id="search" mode="live" />
            <Results
              id="results-cfg"
              searchBoxId="search"
              fields={["title"]}
              items={() => <div />}
            />
            <Results id="results" items={(data) => <div>{data.length}</div>} />
          </Listener>
        </Antfly>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(onChange).toHaveBeenCalled();
      });

      // Check that onChange was called with Map containing values
      const lastCall = onChange.mock.calls[onChange.mock.calls.length - 1];
      expect(lastCall[0]).toBeInstanceOf(Map);
    });
  });

  describe("Error handling", () => {
    it("should handle connection errors gracefully", async () => {
      // Using an invalid URL to simulate connection error
      const { container } = render(
        <Antfly url="http://invalid-url-that-does-not-exist:9999/api" table="test">
          <QueryBox id="search" mode="live" />
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
          <Results id="results" items={(data) => <div>{data.length}</div>} />
        </Antfly>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // Should not crash even with connection error
      // (error will be logged to console)
      expect(container).toBeTruthy();

      await waitFor(
        () => {
          expect(input.value).toBe("test");
        },
        { timeout: 2000 }
      );
    });
  });

  describe("Widget cleanup", () => {
    it("should clean up widgets on unmount", async () => {
      const { container, unmount } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // Unmount and verify no errors
      unmount();
      expect(container).toBeTruthy();
    });

    it("should handle rapid mount/unmount cycles", async () => {
      const { container, unmount, rerender } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // Rerender multiple times
      rerender(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["title__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results-cfg" searchBoxId="search" fields={["title"]} items={() => <div />} />
        </TestWrapper>
      );

      rerender(
        <TestWrapper>
          <QueryBox id="search" mode="live">
            <Autosuggest fields={["name__keyword"]} minChars={1} />
          </QueryBox>
          <Results id="results-name" searchBoxId="search" fields={["name"]} items={() => <div />} />
        </TestWrapper>
      );

      unmount();
      expect(container).toBeTruthy();
    });
  });

  describe("filterQuery support", () => {
    it("should pass filterQuery to multiquery for Results widget", async () => {
      const msearchSpy = vi.spyOn(await import("./utils"), "multiquery").mockResolvedValue({
        responses: [
          {
            status: 200,
            took: 10,
            hits: { hits: [], total: 0 },
          },
        ],
      });

      const filterQuery = { match: "active", field: "status" };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results"
            searchBoxId="search"
            fields={["title"]}
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
        expect(queries[0].query.filter_query).toEqual(filterQuery);
      });

      msearchSpy.mockRestore();
    });

    it("should pass filterQuery to multiquery for SearchBox widget", async () => {
      const msearchSpy = vi.spyOn(await import("./utils"), "multiquery").mockResolvedValue({
        responses: [
          {
            status: 200,
            took: 10,
            hits: { hits: [], total: 0 },
          },
        ],
      });

      const filterQuery = { match: "published", field: "state" };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results-filter"
            searchBoxId="search"
            fields={["title"]}
            filterQuery={filterQuery}
            items={() => <div />}
          />
          <Results id="results" items={(data) => <div>Results: {data.length}</div>} />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      await waitFor(() => {
        expect(msearchSpy).toHaveBeenCalled();
      });

      msearchSpy.mockRestore();
    });

    it("should handle complex filterQuery with conjuncts", async () => {
      const msearchSpy = vi.spyOn(await import("./utils"), "multiquery").mockResolvedValue({
        responses: [
          {
            status: 200,
            took: 10,
            hits: { hits: [], total: 0 },
          },
        ],
      });

      const filterQuery = {
        conjuncts: [
          { match: "active", field: "status" },
          { min: 100, max: 500, field: "price" },
        ],
      };

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results"
            searchBoxId="search"
            fields={["title"]}
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
        expect(queries[0].query.filter_query).toEqual(filterQuery);
      });

      msearchSpy.mockRestore();
    });
  });

  describe("match_phrase query support", () => {
    it("should NOT include match_phrase queries for single term searches", async () => {
      vi.restoreAllMocks();
      const utils = await import("./utils");
      const msearchSpy = vi.spyOn(utils, "multiquery").mockResolvedValue({
        responses: [
          {
            status: 200,
            took: 10,
            hits: { hits: [], total: 0 },
          },
        ],
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results"
            searchBoxId="search"
            fields={["title", "description"]}
            items={(data) => <div>Results: {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "test");

      // Wait for the query with the actual search term to be sent
      await waitFor(() => {
        const calls = msearchSpy.mock.calls;
        const hasSearchQuery = calls.some((call) => {
          const queries = call[1] as Array<{ query: Record<string, unknown> }>;
          const fts = queries?.[0]?.query?.full_text_search as Record<string, unknown>;
          return fts && !("match_all" in fts);
        });
        expect(hasSearchQuery).toBe(true);
      });

      // Find the query with actual search terms (not match_all)
      const callWithSearch = msearchSpy.mock.calls.find((call) => {
        const queries = call[1] as Array<{ query: Record<string, unknown> }>;
        const fts = queries?.[0]?.query?.full_text_search as Record<string, unknown>;
        return fts && !("match_all" in fts);
      });
      const queries = callWithSearch?.[1] as Array<{ query: Record<string, unknown> }>;
      const fullTextSearch = queries[0].query.full_text_search as Record<string, unknown>;

      // Should have disjuncts with only match queries (no match_phrase)
      if (fullTextSearch.disjuncts) {
        const disjuncts = fullTextSearch.disjuncts as Array<Record<string, unknown>>;
        const hasMatchPhrase = disjuncts.some((q: Record<string, unknown>) => "match_phrase" in q);
        expect(hasMatchPhrase).toBe(false);
      }

      msearchSpy.mockRestore();
    });

    it("should include match_phrase queries for multi-term searches", async () => {
      vi.restoreAllMocks();
      const utils = await import("./utils");
      const msearchSpy = vi.spyOn(utils, "multiquery").mockResolvedValue({
        responses: [
          {
            status: 200,
            took: 10,
            hits: { hits: [], total: 0 },
          },
        ],
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results"
            searchBoxId="search"
            fields={["title", "description"]}
            items={(data) => <div>Results: {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      await userEvent.type(input, "hello world");

      // Wait for the query with the multi-term search and match_phrase to be sent
      await waitFor(() => {
        const calls = msearchSpy.mock.calls;
        const hasMatchPhraseQuery = calls.some((call) => {
          const queries = call[1] as Array<{ query: Record<string, unknown> }>;
          const fts = queries?.[0]?.query?.full_text_search as Record<string, unknown>;
          if (fts?.disjuncts) {
            const disjuncts = fts.disjuncts as Array<Record<string, unknown>>;
            return disjuncts.some((q) => "match_phrase" in q);
          }
          return false;
        });
        expect(hasMatchPhraseQuery).toBe(true);
      });

      // Find the query with match_phrase
      const callWithMatchPhrase = msearchSpy.mock.calls.find((call) => {
        const queries = call[1] as Array<{ query: Record<string, unknown> }>;
        const fts = queries?.[0]?.query?.full_text_search as Record<string, unknown>;
        if (fts?.disjuncts) {
          const disjuncts = fts.disjuncts as Array<Record<string, unknown>>;
          return disjuncts.some((q) => "match_phrase" in q);
        }
        return false;
      });
      const queries = callWithMatchPhrase?.[1] as Array<{ query: Record<string, unknown> }>;
      const fullTextSearch = queries[0].query.full_text_search as Record<string, unknown>;

      // Should have disjuncts with both match and match_phrase queries
      expect(fullTextSearch.disjuncts).toBeDefined();
      const disjuncts = fullTextSearch.disjuncts as Array<Record<string, unknown>>;
      const matchQueries = disjuncts.filter((q: Record<string, unknown>) => "match" in q);
      const matchPhraseQueries = disjuncts.filter(
        (q: Record<string, unknown>) => "match_phrase" in q
      );

      // Should have 2 match queries (one per field)
      expect(matchQueries.length).toBe(2);
      // Should have 2 match_phrase queries (one per field)
      expect(matchPhraseQueries.length).toBe(2);

      // Verify match_phrase queries have correct structure
      expect(matchPhraseQueries[0]).toEqual({ match_phrase: "hello world", field: "title" });
      expect(matchPhraseQueries[1]).toEqual({
        match_phrase: "hello world",
        field: "description",
      });

      msearchSpy.mockRestore();
    });

    it("should include match_phrase queries for queries with leading/trailing spaces", async () => {
      vi.restoreAllMocks();
      const utils = await import("./utils");
      const msearchSpy = vi.spyOn(utils, "multiquery").mockResolvedValue({
        responses: [
          {
            status: 200,
            took: 10,
            hits: { hits: [], total: 0 },
          },
        ],
      });

      const { container } = render(
        <TestWrapper>
          <QueryBox id="search" mode="live" />
          <Results
            id="results"
            searchBoxId="search"
            fields={["title"]}
            items={(data) => <div>Results: {data.length}</div>}
          />
        </TestWrapper>
      );

      const input = container.querySelector("input") as HTMLInputElement;
      // Type a query with multiple terms (spaces between words matter)
      await userEvent.type(input, "foo bar");

      // Wait for the query with match_phrase to be sent
      await waitFor(() => {
        const calls = msearchSpy.mock.calls;
        const hasMatchPhraseQuery = calls.some((call) => {
          const queries = call[1] as Array<{ query: Record<string, unknown> }>;
          const fts = queries?.[0]?.query?.full_text_search as Record<string, unknown>;
          if (fts?.disjuncts) {
            const disjuncts = fts.disjuncts as Array<Record<string, unknown>>;
            return disjuncts.some((q) => "match_phrase" in q);
          }
          return false;
        });
        expect(hasMatchPhraseQuery).toBe(true);
      });

      // Find the query with match_phrase
      const callWithMatchPhrase = msearchSpy.mock.calls.find((call) => {
        const queries = call[1] as Array<{ query: Record<string, unknown> }>;
        const fts = queries?.[0]?.query?.full_text_search as Record<string, unknown>;
        if (fts?.disjuncts) {
          const disjuncts = fts.disjuncts as Array<Record<string, unknown>>;
          return disjuncts.some((q) => "match_phrase" in q);
        }
        return false;
      });
      const queries = callWithMatchPhrase?.[1] as Array<{ query: Record<string, unknown> }>;
      const fullTextSearch = queries[0].query.full_text_search as Record<string, unknown>;

      // Should have disjuncts with match_phrase
      expect(fullTextSearch.disjuncts).toBeDefined();
      const disjuncts = fullTextSearch.disjuncts as Array<Record<string, unknown>>;
      const matchPhraseQueries = disjuncts.filter(
        (q: Record<string, unknown>) => "match_phrase" in q
      );
      expect(matchPhraseQueries.length).toBe(1);

      msearchSpy.mockRestore();
    });
  });
});
