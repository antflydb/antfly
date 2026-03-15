import { Streamdown } from "streamdown";
import {
  AnswerResults,
  Antfly,
  Autosuggest,
  AutosuggestFacets,
  AutosuggestResults,
  type GeneratorConfig,
  getCitedDocumentIds,
  QueryBox,
  Results,
  renderAsMarkdownLinks,
  replaceCitations,
} from "../src";
import { tableName, url } from "./utils";

export default {
  title: "RAG (Retrieval-Augmented Generation)",
  component: AnswerResults,
};

// Mock summarizer configuration - replace with your actual config
const mockSummarizer: GeneratorConfig = {
  provider: "ollama",
  model: "gemma3:4b",
  // provider: "openai",
  // model: "gpt-4",
  // api_key: import.meta.env.VITE_OPENAI_API_KEY || "your-api-key-here",
};

export const BasicRAG = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>Basic RAG Example</h1>
      <p>Ask a question and get an AI-generated summary based on search results.</p>
      <pre>{`<QueryBox id="question" />
<AnswerResults
  id="rag-answer"
  searchBoxId="question"
  generator={mockSummarizer}
/>`}</pre>

      <QueryBox id="question" placeholder="Ask a question..." />

      <div style={{ marginTop: "20px" }}>
        <AnswerResults id="rag-answer" searchBoxId="question" generator={mockSummarizer} />
      </div>
    </Antfly>
  );
};

export const WithSystemPrompt = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>RAG with Custom System Prompt</h1>
      <p>Guide the AI's behavior with a custom system prompt.</p>
      <pre>{`<QueryBox id="question" />
<AnswerResults
  id="rag-answer"
  searchBoxId="question"
  generator={mockSummarizer}
  agentKnowledge="You are a literary expert. Provide concise, scholarly answers."
/>`}</pre>

      <QueryBox id="question" placeholder="Ask about literature..." />

      <div style={{ marginTop: "20px" }}>
        <AnswerResults
          id="rag-answer"
          searchBoxId="question"
          generator={mockSummarizer}
          agentKnowledge="You are a literary expert. Provide concise, scholarly answers about books and authors."
        />
      </div>
    </Antfly>
  );
};

export const WithCustomRendering = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>RAG with Custom Rendering</h1>
      <p>Customize how the AI summary is displayed. Parse inline citations yourself!</p>
      <pre>{`<AnswerResults
  id="rag-answer"
  searchBoxId="question"
  generator={mockSummarizer}
  renderAnswer={(summary, isStreaming) => (
    <div style={{
      padding: "20px",
      background: "#f5f5f5",
      borderRadius: "8px"
    }}>
      <h3>AI Answer {isStreaming && "⏳"}</h3>
      <p>{summary}</p>
      {/* Parse [doc_id ...] from summary text as needed */}
    </div>
  )}
/>`}</pre>

      <QueryBox id="question" placeholder="Ask a question..." />

      <div style={{ marginTop: "20px" }}>
        <AnswerResults
          id="rag-answer"
          searchBoxId="question"
          generator={mockSummarizer}
          renderAnswer={(summary, isStreaming) => (
            <div
              style={{
                padding: "20px",
                background: "#f5f5f5",
                borderRadius: "8px",
                border: "2px solid #e0e0e0",
              }}
            >
              <h3 style={{ marginTop: 0 }}>
                AI Answer {isStreaming && <span style={{ color: "#4a90e2" }}>⏳</span>}
              </h3>
              <p style={{ lineHeight: 1.6 }}>{summary || "No answer yet..."}</p>
              <div style={{ marginTop: "15px", fontSize: "14px", color: "#666" }}>
                <em>Citations are inline in the text above (look for [doc_id ...])</em>
              </div>
            </div>
          )}
        />
      </div>
    </Antfly>
  );
};

export const RAGWithSearchResults = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>RAG + Traditional Search Results</h1>
      <p>
        Combine AI-generated summaries with traditional search results for the best of both worlds.
      </p>
      <pre>{`<QueryBox id="question" />

{/* AI Summary with inline citations */}
<AnswerResults
  id="rag-answer"
  searchBoxId="question"
  generator={mockSummarizer}
/>

{/* Traditional Results */}
<Results
  id="search-results"
  items={(data) => data.map(hit => ...)}
/>`}</pre>

      <QueryBox id="question" placeholder="Ask a question..." buttonLabel="Search & Summarize" />

      <div style={{ marginTop: "20px" }}>
        <h2>AI Summary</h2>
        <AnswerResults id="rag-answer" searchBoxId="question" generator={mockSummarizer} />
      </div>

      <div style={{ marginTop: "40px" }}>
        <h2>Search Results</h2>
        <Results
          id="search-results"
          items={(data) =>
            data.map(({ _source: s, _id, _score }) => (
              <div
                key={_id}
                style={{
                  padding: "15px",
                  borderBottom: "1px solid #e0e0e0",
                  marginBottom: "10px",
                }}
              >
                <h4 style={{ margin: "0 0 8px 0" }}>{String(s?.TICO)}</h4>
                <p style={{ margin: "0 0 5px 0", color: "#666" }}>{String(s?.AUTR)}</p>
                <small style={{ color: "#999" }}>Score: {_score?.toFixed(2)}</small>
              </div>
            ))
          }
          pagination={() => <></>}
        />
      </div>
    </Antfly>
  );
};

export const StyledRAGExample = () => {
  // Helper function to parse summary text and convert [doc_id X] to clickable links
  return (
    <Antfly url={url} table={tableName}>
      <h1>Styled RAG Interface</h1>
      <p style={{ marginBottom: "20px", fontSize: "16px", lineHeight: "1.6" }}>
        This example demonstrates a fully-styled RAG interface using{" "}
        <a href="https://streamdown.ai/" target="_blank" rel="noopener noreferrer">
          Streamdown.ai
        </a>{" "}
        for rich markdown rendering (code highlighting, math, GFM) with interactive citations that
        scroll to source documents.
      </p>
      <style>{`
        .rag-container {
          max-width: 1200px;
          margin: 0 auto;
        }
        .react-af-querybox form {
          display: flex;
          gap: 10px;
          margin: 20px 0;
        }
        .react-af-querybox input {
          flex: 1;
          padding: 14px;
          border: 2px solid #4a90e2;
          border-radius: 8px;
          font-size: 16px;
        }
        .react-af-querybox button {
          padding: 14px 28px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 16px;
          font-weight: 600;
          cursor: pointer;
        }
        .react-af-querybox button:hover:not(:disabled) {
          background: #357abd;
        }
        .rag-summary-section {
          background: white;
          padding: 24px;
          border-radius: 12px;
          box-shadow: 0 2px 8px rgba(0,0,0,0.1);
          margin-bottom: 24px;
        }
        .rag-summary-text {
          line-height: 1.8;
          color: #333;
          font-size: 16px;
        }
        .rag-inline-citation-link {
          color: #4a90e2;
          text-decoration: none;
          font-weight: 600;
          padding: 1px 4px;
          border-radius: 3px;
          transition: background 0.2s;
        }
        .rag-inline-citation-link:hover {
          background: #e8f4ff;
          text-decoration: underline;
        }
        .rag-streaming-indicator {
          color: #4a90e2;
          font-weight: bold;
        }
        .rag-citations {
          margin-top: 20px;
          padding-top: 20px;
          border-top: 2px solid #f0f0f0;
        }
        .rag-citations h3 {
          font-size: 18px;
          color: #4a90e2;
          margin-bottom: 12px;
        }
        .rag-citation-item {
          margin: 10px 0;
          padding: 12px;
          background: #f8f9fa;
          border-radius: 6px;
        }
        .rag-citation-link {
          color: #4a90e2;
          text-decoration: none;
          font-weight: 600;
        }
        .rag-citation-link:hover {
          text-decoration: underline;
        }
        .rag-citation-quote {
          margin-top: 8px;
          padding: 8px;
          background: white;
          border-left: 3px solid #4a90e2;
          font-size: 14px;
          color: #666;
          font-style: italic;
        }
        .rag-hits-section {
          margin-top: 24px;
        }
        .rag-hits-section h2 {
          font-size: 20px;
          color: #333;
          margin-bottom: 16px;
        }
        .rag-hit {
          padding: 20px;
          margin-bottom: 16px;
          background: white;
          border-radius: 8px;
          box-shadow: 0 1px 4px rgba(0,0,0,0.1);
          border-left: 4px solid #4a90e2;
          scroll-margin-top: 20px;
        }
        .rag-hit:target {
          background: #f0f8ff;
          box-shadow: 0 2px 12px rgba(74, 144, 226, 0.3);
        }
        .rag-hit-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 12px;
        }
        .rag-hit-title {
          font-size: 18px;
          font-weight: 600;
          color: #333;
          margin: 0;
        }
        .rag-hit-score {
          font-size: 12px;
          color: #999;
          background: #f5f5f5;
          padding: 4px 8px;
          border-radius: 4px;
        }
        .rag-hit-content {
          color: #666;
          line-height: 1.6;
        }
        .rag-error {
          padding: 16px;
          background: #fee;
          border-left: 4px solid #f44;
          border-radius: 4px;
        }
        .rag-empty {
          padding: 24px;
          text-align: center;
          color: #999;
          font-style: italic;
        }

        /* Autosuggest styles */
        .react-af-querybox {
          position: relative;
        }

        /* Legacy autosuggest */
        .react-af-autosuggest {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          background: white;
          border: 2px solid #4a90e2;
          border-top: none;
          border-radius: 0 0 8px 8px;
          max-height: 300px;
          overflow-y: auto;
          list-style: none;
          padding: 0;
          margin: 0;
          box-shadow: 0 4px 8px rgba(0,0,0,0.1);
          z-index: 1000;
        }
        .react-af-autosuggest-item {
          padding: 12px 14px;
          cursor: pointer;
          border-bottom: 1px solid #f0f0f0;
          transition: background 0.2s;
        }
        .react-af-autosuggest-item:last-child {
          border-bottom: none;
        }
        .react-af-autosuggest-item:hover,
        .react-af-autosuggest-item-selected {
          background: #f0f8ff;
        }

        /* Composable autosuggest */
        .react-af-autosuggest-container {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          background: white;
          border: 2px solid #4a90e2;
          border-top: none;
          border-radius: 0 0 8px 8px;
          max-height: 400px;
          overflow-y: auto;
          box-shadow: 0 4px 8px rgba(0,0,0,0.1);
          z-index: 1000;
        }

        /* Horizontal layout for side-by-side facets and results */
        .react-af-autosuggest-layout-horizontal .react-af-autosuggest-dropdown {
          display: flex;
          flex-direction: row;
        }

        .react-af-autosuggest-layout-horizontal .react-af-autosuggest-facets {
          flex: 0 0 250px;
          border-right: 2px solid #e0e0e0;
          border-top: none;
        }

        .react-af-autosuggest-layout-horizontal .react-af-autosuggest-results {
          flex: 1;
        }

        .react-af-autosuggest-results-list {
          list-style: none;
          padding: 0;
          margin: 0;
        }

        .react-af-autosuggest-result-item {
          padding: 12px 14px;
          cursor: pointer;
          border-bottom: 1px solid #f0f0f0;
          transition: background 0.2s;
        }

        .react-af-autosuggest-result-item:hover,
        .react-af-autosuggest-result-item.react-af-autosuggest-item-selected {
          background: #f0f8ff;
        }

        .react-af-autosuggest-facets {
          background: #fafafa;
          border-top: 2px solid #e0e0e0;
        }

        .react-af-autosuggest-facet-section-label {
          padding: 10px 14px;
          font-weight: 600;
          font-size: 13px;
          color: #4a90e2;
          background: #f5f5f5;
          border-bottom: 1px solid #e0e0e0;
        }

        .react-af-autosuggest-facets-list {
          list-style: none;
          padding: 0;
          margin: 0;
        }

        .react-af-autosuggest-facet-item {
          padding: 8px 14px;
          cursor: pointer;
          border-bottom: 1px solid #f0f0f0;
          transition: background 0.2s;
          font-size: 13px;
        }

        .react-af-autosuggest-facet-item:hover,
        .react-af-autosuggest-facet-item.react-af-autosuggest-item-selected {
          background: #e8f4ff;
        }

        .react-af-autosuggest-facet-term {
          display: flex;
          justify-content: space-between;
          color: #333;
        }

        .react-af-autosuggest-facet-count {
          color: #999;
          font-size: 11px;
        }

        .rag-autosuggest-item {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }
        .rag-autosuggest-item strong {
          color: #333;
          font-size: 14px;
        }
        .rag-autosuggest-item small {
          color: #666;
          font-size: 12px;
        }
      `}</style>

      <div className="rag-container">
        <QueryBox
          id="question"
          placeholder="Ask me anything about the books..."
          buttonLabel="Get AI Answer"
        >
          <Autosuggest
            fields={[
              "TICO",
              "AUTR",
              "TICO__2gram",
              "TICO__keyword",
              "AUTR__2gram",
              "AUTR__keyword",
            ]}
            returnFields={["TICO", "AUTR", "DESC"]}
            minChars={2}
            layout="horizontal"
          >
            <AutosuggestFacets
              field="AUTR__keyword"
              size={5}
              label="Filter by Author"
              order="count"
            />
            <AutosuggestResults
              limit={5}
              renderItem={(hit) => (
                <div className="rag-autosuggest-item">
                  <strong>{String(hit._source?.TICO || "")}</strong>
                  <small>{String(hit._source?.AUTR || "")}</small>
                </div>
              )}
            />
          </Autosuggest>
        </QueryBox>

        <AnswerResults
          id="rag-answer"
          searchBoxId="question"
          generator={mockSummarizer}
          showHits={true}
          agentKnowledge="You are a knowledgeable librarian. Provide helpful, detailed answers about books."
          renderAnswer={(summary, isStreaming, hits) => {
            // Convert [doc_id X] or [doc_id X, Y, Z] citations to markdown links before passing to Streamdown
            const summaryWithLinks = summary
              ? replaceCitations(summary, {
                  renderCitation: renderAsMarkdownLinks,
                })
              : "";

            // Handle click events on citation links to scroll instead of navigate
            const handleCitationClick = (e: React.MouseEvent<HTMLDivElement>) => {
              const target = e.target as HTMLElement;
              if (target.tagName === "A" && target.getAttribute("href")?.startsWith("#hit-")) {
                e.preventDefault();
                const id = target.getAttribute("href")?.substring(1);
                if (id) {
                  const element = document.getElementById(id);
                  if (element) {
                    element.scrollIntoView({ behavior: "smooth", block: "start" });
                    element.style.transition = "background 0.3s";
                    element.style.background = "#f0f8ff";
                    setTimeout(() => {
                      element.style.background = "";
                    }, 2000);
                  }
                }
              }
            };

            return (
              <div>
                <div className="rag-summary-section">
                  {!summary && !isStreaming && (
                    <div className="rag-empty">
                      No results yet. Submit a question to get started.
                    </div>
                  )}
                  {summary && (
                    <div>
                      <div
                        role="button"
                        tabIndex={0}
                        className="rag-summary-text"
                        onClick={handleCitationClick}
                        onKeyDown={(e) =>
                          e.key === "Enter" &&
                          handleCitationClick(e as unknown as React.MouseEvent<HTMLDivElement>)
                        }
                      >
                        <Streamdown isAnimating={isStreaming}>{summaryWithLinks}</Streamdown>
                      </div>
                      <div
                        style={{
                          marginTop: "12px",
                          fontSize: "14px",
                          color: "#666",
                          fontStyle: "italic",
                        }}
                      >
                        💡 Tip: Click citation links (e.g., [19], [16]) to jump to source documents
                        below (powered by{" "}
                        <a href="https://streamdown.ai/" target="_blank" rel="noopener">
                          Streamdown.ai
                        </a>
                        )
                      </div>
                    </div>
                  )}
                </div>

                {hits &&
                  hits.length > 0 &&
                  (() => {
                    // Filter hits to only show cited documents
                    const citedIds = getCitedDocumentIds(summary);
                    const citedHits = hits.filter((hit) => citedIds.includes(hit._id));

                    return (
                      <div className="rag-hits-section">
                        <h2>🔍 Cited Documents ({citedHits.length})</h2>
                        {citedHits.map((hit) => (
                          <div key={hit._id} id={`hit-${hit._id}`} className="rag-hit">
                            <div className="rag-hit-header">
                              <h3 className="rag-hit-title">
                                {String(hit._source?.TICO) || `Document ${hit._id}`}
                              </h3>
                              <span className="rag-hit-score">Score: {hit._score.toFixed(3)}</span>
                            </div>
                            <div className="rag-hit-content">
                              {hit._source?.AUTR ? (
                                <p>
                                  <strong>Author:</strong> {String(hit._source?.AUTR)}
                                </p>
                              ) : null}
                              {hit._source?.DESC ? <p>{String(hit._source?.DESC)}</p> : null}
                            </div>
                          </div>
                        ))}
                      </div>
                    );
                  })()}
              </div>
            );
          }}
        />
      </div>
    </Antfly>
  );
};

export const ComposableAutosuggestWithFacets = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>Composable Autosuggest with Facets</h1>
      <p style={{ marginBottom: "20px", fontSize: "16px", lineHeight: "1.6" }}>
        This example demonstrates the new composable Autosuggest API that allows you to mix search
        results with faceted navigation. Users can see both matching documents and popular filter
        options as they type.
      </p>
      <style>{`
        .composable-demo-container {
          max-width: 800px;
          margin: 0 auto;
        }
        .composable-demo-answerbox {
          margin: 20px 0;
        }
        .composable-demo-answerbox form {
          display: flex;
          gap: 10px;
        }
        .composable-demo-answerbox input {
          flex: 1;
          padding: 14px;
          border: 2px solid #4a90e2;
          border-radius: 8px;
          font-size: 16px;
        }
        .composable-demo-answerbox button {
          padding: 14px 28px;
          background: #4a90e2;
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 16px;
          font-weight: 600;
          cursor: pointer;
        }
        .composable-demo-answerbox button:hover {
          background: #357abd;
        }
        .composable-demo-answerbox {
          position: relative;
        }
        .composable-demo-answerbox .react-af-autosuggest-container {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          background: white;
          border: 2px solid #4a90e2;
          border-top: none;
          border-radius: 0 0 8px 8px;
          max-height: 400px;
          overflow-y: auto;
          box-shadow: 0 4px 12px rgba(0,0,0,0.15);
          z-index: 1000;
        }
        .composable-demo-answerbox .react-af-autosuggest-results {
          margin-bottom: 0;
        }
        .composable-demo-answerbox .react-af-autosuggest-results-list {
          list-style: none;
          padding: 0;
          margin: 0;
        }
        .composable-demo-answerbox .react-af-autosuggest-result-item {
          padding: 12px 16px;
          cursor: pointer;
          border-bottom: 1px solid #f0f0f0;
          transition: all 0.2s;
        }
        .composable-demo-answerbox .react-af-autosuggest-result-item:hover,
        .composable-demo-answerbox .react-af-autosuggest-result-item.react-af-autosuggest-item-selected {
          background: #f0f8ff;
        }
        .composable-demo-answerbox .react-af-autosuggest-container {
          display: flex;
          flex-direction: row;
        }
        .composable-demo-answerbox .react-af-autosuggest-results {
          flex: 1;
          border-right: 2px solid #e0e0e0;
        }
        .composable-demo-answerbox .react-af-autosuggest-facets {
          width: 250px;
          background: #fafafa;
        }
        .composable-demo-answerbox .react-af-autosuggest-facet-section-label {
          padding: 12px 16px;
          font-weight: 600;
          font-size: 14px;
          color: #4a90e2;
          background: #f5f5f5;
          border-bottom: 1px solid #e0e0e0;
        }
        .composable-demo-answerbox .react-af-autosuggest-facets-list {
          list-style: none;
          padding: 0;
          margin: 0;
        }
        .composable-demo-answerbox .react-af-autosuggest-facet-item {
          padding: 10px 16px;
          cursor: pointer;
          border-bottom: 1px solid #f0f0f0;
          transition: all 0.2s;
          font-size: 14px;
        }
        .composable-demo-answerbox .react-af-autosuggest-facet-item:hover,
        .composable-demo-answerbox .react-af-autosuggest-facet-item.react-af-autosuggest-item-selected {
          background: #e8f4ff;
        }
        .composable-demo-answerbox .react-af-autosuggest-facet-term {
          display: flex;
          justify-content: space-between;
          color: #333;
        }
        .composable-demo-answerbox .react-af-autosuggest-facet-count {
          color: #999;
          font-size: 12px;
          background: #fff;
          padding: 2px 6px;
          border-radius: 10px;
          margin-left: 8px;
        }
        .demo-result-item {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }
        .demo-result-item strong {
          color: #333;
          font-size: 14px;
        }
        .demo-result-item small {
          color: #666;
          font-size: 12px;
        }
        .code-example {
          background: #f5f5f5;
          padding: 16px;
          border-radius: 8px;
          margin: 20px 0;
          overflow-x: auto;
        }
        .code-example pre {
          margin: 0;
          font-size: 13px;
          line-height: 1.6;
        }
      `}</style>

      <div className="composable-demo-container">
        <div className="code-example">
          <pre>{`<Autosuggest fields={["TICO__keyword", "AUTR__keyword"]} minChars={2}>
  <AutosuggestResults
    limit={5}
    renderItem={(hit) => (
      <div>
        <strong>{hit._source?.TICO}</strong>
        <small>{hit._source?.AUTR}</small>
      </div>
    )}
  />
  <AutosuggestFacets
    field="AUTR__keyword"
    size={5}
    label="Filter by Author"
    order="count"
  />
</Autosuggest>`}</pre>
        </div>

        <div className="composable-demo-answerbox">
          <QueryBox
            id="composable-question"
            placeholder="Start typing to see suggestions and facets..."
            buttonLabel="Search"
          >
            <Autosuggest fields={["TICO__keyword", "AUTR__keyword"]} minChars={2}>
              <AutosuggestResults
                limit={5}
                renderItem={(hit) => (
                  <div className="demo-result-item">
                    <strong>{String(hit._source?.TICO || "")}</strong>
                    <small>by {String(hit._source?.AUTR || "Unknown")}</small>
                  </div>
                )}
              />
              <AutosuggestFacets
                field="AUTR__keyword"
                size={5}
                label="📚 Popular Authors"
                order="count"
              />
            </Autosuggest>
          </QueryBox>
        </div>

        <div style={{ marginTop: "40px" }}>
          <h2>Key Features</h2>
          <ul style={{ lineHeight: "1.8", fontSize: "16px" }}>
            <li>
              <strong>Composable API:</strong> Mix and match <code>AutosuggestResults</code> and{" "}
              <code>AutosuggestFacets</code> components
            </li>
            <li>
              <strong>Flexible Ordering:</strong> Control the order by arranging child components in
              JSX
            </li>
            <li>
              <strong>Custom Layouts:</strong> Support for vertical, horizontal, grid, and custom
              layouts
            </li>
            <li>
              <strong>Separate Limits:</strong> Results and facets have independent size limits
            </li>
            <li>
              <strong>Facet Sorting:</strong> Sort by count, term, or reverse order
            </li>
            <li>
              <strong>Unified Navigation:</strong> Arrow keys navigate across both results and
              facets
            </li>
            <li>
              <strong>Clickable Facets:</strong> Clicking a facet term populates the search bar
            </li>
            <li>
              <strong>Backward Compatible:</strong> Legacy Autosuggest API still works
            </li>
          </ul>
        </div>

        <div style={{ marginTop: "40px" }}>
          <h2>More Examples</h2>
          <h3>Multiple Facets</h3>
          <div className="code-example">
            <pre>{`<Autosuggest fields={["TICO__keyword"]}>
  <AutosuggestResults limit={3} header="Top Matches" />
  <AutosuggestFacets field="AUTR__keyword" size={3} label="Authors" />
  <AutosuggestFacets field="LANG__keyword" size={3} label="Languages" />
</Autosuggest>`}</pre>
          </div>

          <h3>Custom Ordering</h3>
          <div className="code-example">
            <pre>{`<Autosuggest fields={["TICO__keyword"]}>
  <AutosuggestResults limit={2} />
  <AutosuggestFacets field="AUTR__keyword" size={2} />
  <AutosuggestResults limit={3} header="More Results" />
</Autosuggest>`}</pre>
          </div>
        </div>
      </div>
    </Antfly>
  );
};

export const MultipleLanguageModels = () => {
  const gpt4Summarizer: GeneratorConfig = {
    provider: "openai",
    model: "gpt-4",
    api_key:
      (import.meta as { env?: Record<string, string> }).env?.VITE_OPENAI_API_KEY || "your-api-key",
  };

  const claudeSummarizer: GeneratorConfig = {
    provider: "openai" as "openai" | "ollama", // anthropic not yet supported in type
    model: "claude-3-sonnet-20240229",
    api_key:
      (import.meta as { env?: Record<string, string> }).env?.VITE_ANTHROPIC_API_KEY ||
      "your-api-key",
  };

  return (
    <Antfly url={url} table={tableName}>
      <h1>Compare Different Language Models</h1>
      <p>See how different models respond to the same question.</p>

      <QueryBox id="question" placeholder="Ask a question..." />

      <div
        style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "20px", marginTop: "20px" }}
      >
        <div>
          <h3>GPT-4 Response</h3>
          <AnswerResults id="rag-gpt4" searchBoxId="question" generator={gpt4Summarizer} />
        </div>
        <div>
          <h3>Claude Response</h3>
          <AnswerResults id="rag-claude" searchBoxId="question" generator={claudeSummarizer} />
        </div>
      </div>
    </Antfly>
  );
};
