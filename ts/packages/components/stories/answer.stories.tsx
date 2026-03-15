import type React from "react";
import { Streamdown } from "streamdown";
import {
  AnswerFeedback,
  AnswerResults,
  Antfly,
  Autosuggest,
  AutosuggestFacets,
  AutosuggestResults,
  type GeneratorConfig,
  getCitedDocumentIds,
  QueryBox,
  renderAsMarkdownLinks,
  renderThumbsUpDown,
  replaceCitations,
} from "../src";
import { tableName, url } from "./utils";

export default {
  title: "Answer Agent",
  component: AnswerResults,
};

// Mock generator configuration - replace with your actual config
const mockGenerator: GeneratorConfig = {
  provider: "ollama",
  model: "gemma3:4b",
  // provider: "openai",
  // model: "gpt-4",
  // api_key: import.meta.env.VITE_OPENAI_API_KEY || "your-api-key-here",
};

export const BasicAnswer = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>Basic Answer Agent</h1>
      <p>Ask a question and get an AI-generated answer with intelligent query routing.</p>
      <pre>{`<QueryBox id="question" />
<AnswerResults
  id="answer"
  searchBoxId="question"
  generator={mockGenerator}
/>`}</pre>

      <QueryBox id="question" placeholder="Ask a question..." />

      <div style={{ marginTop: "20px" }}>
        <AnswerResults id="answer" searchBoxId="question" generator={mockGenerator} />
      </div>
    </Antfly>
  );
};

export const WithAllInsights = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>Answer Agent with All Insights</h1>
      <p>See classification, keywords, generated query, reasoning, and follow-up questions.</p>
      <pre>{`<QueryBox id="question" />
<AnswerResults
  id="answer"
  searchBoxId="question"
  generator={mockGenerator}
  showClassification={true}
  showKeywords={true}
  showQueryGenerated={true}
  showReasoning={true}
  showFollowUpQuestions={true}
/>`}</pre>

      <QueryBox id="question" placeholder="Ask a question..." />

      <div style={{ marginTop: "20px" }}>
        <AnswerResults
          id="answer"
          searchBoxId="question"
          generator={mockGenerator}
          showClassification={true}
          showReasoning={true}
          showFollowUpQuestions={true}
        />
      </div>
    </Antfly>
  );
};

export const WithCustomRenderers = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>Answer Agent with Custom Renderers</h1>
      <p>Customize how each part of the answer is displayed.</p>

      <QueryBox id="question" placeholder="Ask a question..." />

      <div style={{ marginTop: "20px" }}>
        <AnswerResults
          id="answer"
          searchBoxId="question"
          generator={mockGenerator}
          showClassification={true}
          showReasoning={true}
          showFollowUpQuestions={true}
          renderClassification={(data) => (
            <div
              style={{
                padding: "10px",
                background: "#e3f2fd",
                borderRadius: "4px",
                marginBottom: "10px",
              }}
            >
              <strong>🎯 Query Type:</strong> {data.route_type}{" "}
              <span style={{ color: "#666" }}>
                ({(data.confidence * 100).toFixed(0)}% confident)
              </span>
              <div style={{ marginTop: "8px", fontSize: "14px" }}>
                <div>
                  <strong>Improved:</strong> {data.improved_query}
                </div>
                <div>
                  <strong>Semantic:</strong> {data.semantic_query}
                </div>
              </div>
            </div>
          )}
          renderReasoning={(reasoning, isStreaming) => (
            <details
              open
              style={{
                padding: "10px",
                background: "#f3e5f5",
                borderRadius: "4px",
                marginBottom: "10px",
              }}
            >
              <summary style={{ cursor: "pointer", fontWeight: "bold" }}>
                🧠 Reasoning{isStreaming && " (streaming...)"}
              </summary>
              <p style={{ marginTop: "10px", fontSize: "14px", color: "#555" }}>{reasoning}</p>
            </details>
          )}
          renderAnswer={(answer, isStreaming) => (
            <div
              style={{
                padding: "20px",
                background: "#e8f5e9",
                borderRadius: "8px",
                marginBottom: "10px",
              }}
            >
              <h3 style={{ margin: "0 0 10px 0", color: "#2e7d32" }}>
                💡 Answer{isStreaming && " (streaming...)"}
              </h3>
              <div style={{ fontSize: "16px", lineHeight: "1.6" }}>{answer}</div>
            </div>
          )}
          renderFollowUpQuestions={(questions) => (
            <div style={{ padding: "15px", background: "#fce4ec", borderRadius: "4px" }}>
              <strong>❓ Suggested Follow-ups:</strong>
              <ul style={{ marginTop: "10px", paddingLeft: "20px" }}>
                {questions.map((q) => (
                  <li key={q} style={{ marginBottom: "8px", cursor: "pointer", color: "#c2185b" }}>
                    {q}
                  </li>
                ))}
              </ul>
            </div>
          )}
        />
      </div>
    </Antfly>
  );
};

export const WithAgentKnowledge = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>Answer Agent with Agent Knowledge</h1>
      <p>Guide the AI's behavior with custom agent knowledge context.</p>
      <pre>{`<QueryBox id="question" />
<AnswerResults
  id="answer"
  searchBoxId="question"
  generator={mockGenerator}
  agentKnowledge="You are a literary expert. Provide concise, scholarly answers with historical context."
  showReasoning={true}
/>`}</pre>

      <QueryBox id="question" placeholder="Ask about literature..." />

      <div style={{ marginTop: "20px" }}>
        <AnswerResults
          id="answer"
          searchBoxId="question"
          generator={mockGenerator}
          agentKnowledge="You are a literary expert. Provide concise, scholarly answers with historical context."
          showReasoning={true}
        />
      </div>
    </Antfly>
  );
};

export const WithFeedback = () => {
  const handleFeedback = (data: {
    feedback: { rating: number; scale: number; comment?: string };
    result: unknown;
    query: string;
    context?: {
      classification?: { route_type: "question" | "search"; confidence: number };
      keywords?: string[];
      reasoning?: string;
    };
  }) => {
    console.log("Feedback received:", data);
    alert(
      `Feedback submitted!\nRating: ${data.feedback.rating}/${data.feedback.scale}\nQuery: ${data.query}\n${data.feedback.comment ? `Comment: ${data.feedback.comment}` : ""}`
    );
  };

  return (
    <Antfly url={url} table={tableName}>
      <h1>Answer Agent with Feedback</h1>
      <p>Collect user feedback on answer quality with rich context.</p>

      <QueryBox id="question" placeholder="Ask a question..." />

      <div style={{ marginTop: "20px" }}>
        <AnswerResults
          id="answer"
          searchBoxId="question"
          generator={mockGenerator}
          showReasoning={true}
        >
          <div
            style={{
              marginTop: "20px",
              padding: "15px",
              background: "#f5f5f5",
              borderRadius: "8px",
            }}
          >
            <AnswerFeedback
              scale={1}
              renderRating={renderThumbsUpDown}
              onFeedback={handleFeedback}
            />
          </div>
        </AnswerResults>
      </div>
    </Antfly>
  );
};

export const WithHits = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>Answer Agent with Search Results</h1>
      <p>Show the underlying search results that informed the answer.</p>
      <pre>{`<QueryBox id="question" />
<AnswerResults
  id="answer"
  searchBoxId="question"
  generator={mockGenerator}
  showHits={true}
/>`}</pre>

      <QueryBox id="question" placeholder="Ask a question..." />

      <div style={{ marginTop: "20px" }}>
        <AnswerResults
          id="answer"
          searchBoxId="question"
          generator={mockGenerator}
          showHits={true}
        />
      </div>
    </Antfly>
  );
};

export const StyledExample = () => {
  const handleFeedback = (data: {
    feedback: { rating: number; scale: number; comment?: string };
    result: unknown;
    query: string;
    context?: {
      classification?: { route_type: "question" | "search"; confidence: number };
      reasoning?: string;
    };
  }) => {
    console.log("Feedback received:", data);
    alert(
      `Thanks for your feedback!\n\nRating: ${data.feedback.rating === 1 ? "👍 Helpful" : "👎 Not Helpful"}\nQuery: ${data.query}\n${data.feedback.comment ? `Comment: ${data.feedback.comment}` : ""}`
    );
  };

  return (
    <Antfly url={url} table={tableName}>
      <h1>Styled Answer Agent</h1>
      <p style={{ marginBottom: "20px", fontSize: "16px", lineHeight: "1.6" }}>
        This example demonstrates a fully-styled Answer Agent interface using{" "}
        <a href="https://streamdown.ai/" target="_blank" rel="noopener noreferrer">
          streamdown
        </a>{" "}
        for rich markdown rendering with code highlighting, math support, and interactive citations.
      </p>
      <style>{`
        .answer-container {
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
          padding: 15px;
          border: 2px solid #667eea;
          border-radius: 12px;
          font-size: 16px;
        }
        .react-af-querybox input:focus {
          outline: none;
          border-color: #764ba2;
          box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        .react-af-querybox button {
          padding: 15px 30px;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          color: white;
          border: none;
          border-radius: 12px;
          font-size: 16px;
          font-weight: 600;
          cursor: pointer;
          transition: transform 0.2s;
        }
        .react-af-querybox button:hover:not(:disabled) {
          transform: translateY(-2px);
        }
        .react-af-querybox button:disabled {
          background: #ccc;
          cursor: not-allowed;
          transform: none;
        }
        .answer-section {
          background: white;
          padding: 24px;
          border-radius: 12px;
          box-shadow: 0 4px 12px rgba(102, 126, 234, 0.15);
          margin-bottom: 24px;
        }
        .answer-text {
          font-size: 16px;
          line-height: 1.8;
          color: #333;
        }
        .answer-inline-citation-link {
          color: #667eea;
          text-decoration: none;
          font-weight: 600;
          padding: 1px 4px;
          border-radius: 3px;
          transition: background 0.2s;
        }
        .answer-inline-citation-link:hover {
          background: #f0f3ff;
          text-decoration: underline;
        }
        .answer-streaming-indicator {
          color: #667eea;
          font-weight: bold;
        }
        .answer-classification {
          padding: 16px;
          background: linear-gradient(135deg, #f0f3ff 0%, #faf5ff 100%);
          border-left: 4px solid #667eea;
          border-radius: 8px;
          margin-bottom: 20px;
          font-size: 14px;
        }
        .answer-reasoning {
          padding: 16px;
          background: #faf5ff;
          border-left: 4px solid #764ba2;
          border-radius: 8px;
          margin-bottom: 20px;
        }
        .answer-followup {
          padding: 16px;
          background: #f0f9ff;
          border-radius: 8px;
          margin-top: 20px;
        }
        .answer-followup h3 {
          margin: 0 0 12px 0;
          color: #667eea;
          font-size: 16px;
        }
        .answer-followup ul {
          list-style: none;
          padding: 0;
          margin: 0;
        }
        .answer-followup li {
          padding: 10px;
          margin: 6px 0;
          background: white;
          border-radius: 6px;
          cursor: pointer;
          transition: all 0.2s;
          color: #667eea;
        }
        .answer-followup li:hover {
          background: #e0e7ff;
          transform: translateX(4px);
        }
        .answer-hits-section {
          margin-top: 24px;
        }
        .answer-hits-section h2 {
          font-size: 20px;
          color: #333;
          margin-bottom: 16px;
        }
        .answer-hit {
          padding: 20px;
          margin-bottom: 16px;
          background: white;
          border-radius: 8px;
          box-shadow: 0 2px 8px rgba(0,0,0,0.1);
          border-left: 4px solid #667eea;
          scroll-margin-top: 20px;
        }
        .answer-hit:target {
          background: #f0f3ff;
          box-shadow: 0 4px 16px rgba(102, 126, 234, 0.3);
        }
        .answer-hit-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 12px;
        }
        .answer-hit-title {
          font-size: 18px;
          font-weight: 600;
          color: #333;
          margin: 0;
        }
        .answer-hit-score {
          font-size: 12px;
          color: #999;
          background: #f5f5f5;
          padding: 4px 8px;
          border-radius: 4px;
        }
        .answer-hit-content {
          color: #666;
          line-height: 1.6;
        }
        .answer-empty {
          padding: 24px;
          text-align: center;
          color: #999;
          font-style: italic;
        }

        /* Feedback styles */
        .react-af-answer-feedback {
          margin-top: 24px;
          padding: 20px;
          background: linear-gradient(135deg, #f0f3ff 0%, #faf5ff 100%);
          border-radius: 12px;
          border: 2px solid #667eea;
        }
        .react-af-answer-feedback h4,
        .react-af-answer-feedback .react-af-feedback-heading {
          margin: 0 0 16px 0;
          color: #667eea;
          font-size: 16px;
        }
        .react-af-feedback-thumbs {
          display: flex;
          gap: 12px;
          margin-bottom: 16px;
        }
        .react-af-feedback-thumb-up,
        .react-af-feedback-thumb-down {
          padding: 12px 24px;
          border: 2px solid #667eea;
          background: white;
          border-radius: 8px;
          cursor: pointer;
          font-size: 24px;
          transition: all 0.2s;
        }
        .react-af-feedback-thumb-up:hover,
        .react-af-feedback-thumb-down:hover {
          background: #667eea;
          transform: scale(1.1);
        }
        .react-af-feedback-thumb-up.active,
        .react-af-feedback-thumb-down.active {
          background: #667eea;
          color: white;
        }
        .react-af-feedback-comment-input {
          width: 100%;
          padding: 12px;
          border: 2px solid #667eea;
          border-radius: 8px;
          font-size: 14px;
          font-family: inherit;
          resize: vertical;
          margin-bottom: 12px;
        }
        .react-af-feedback-submit {
          padding: 12px 24px;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          color: white;
          border: none;
          border-radius: 8px;
          font-size: 14px;
          font-weight: 600;
          cursor: pointer;
          transition: transform 0.2s;
        }
        .react-af-feedback-submit:hover {
          transform: translateY(-2px);
        }

        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.3; }
        }

        /* Autosuggest styles */
        .react-af-querybox {
          position: relative;
        }

        .react-af-querybox .react-af-autosuggest-container {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          background: white;
          border: 2px solid #667eea;
          border-top: none;
          border-radius: 0 0 12px 12px;
          max-height: 400px;
          overflow-y: auto;
          box-shadow: 0 4px 12px rgba(102, 126, 234, 0.2);
          z-index: 1000;
          opacity: 0;
          animation: fadeIn 0.15s ease-in forwards;
          animation-delay: 0.1s;
        }

        @keyframes fadeIn {
          from {
            opacity: 0;
            transform: translateY(-5px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
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
          background: #fafafa;
        }

        .react-af-autosuggest-layout-horizontal .react-af-autosuggest-results {
          flex: 1;
        }

        .react-af-querybox .react-af-autosuggest-results-list {
          list-style: none;
          padding: 0;
          margin: 0;
        }

        .react-af-querybox .react-af-autosuggest-result-item {
          padding: 12px 16px;
          cursor: pointer;
          border-bottom: 1px solid #f0f0f0;
          transition: all 0.2s;
        }

        .react-af-querybox .react-af-autosuggest-result-item:hover,
        .react-af-querybox .react-af-autosuggest-result-item.react-af-autosuggest-item-selected {
          background: #f0f3ff;
        }

        .react-af-querybox .react-af-autosuggest-facet-section-label {
          padding: 12px 16px;
          font-weight: 600;
          font-size: 14px;
          color: #667eea;
          background: #f5f5f5;
          border-bottom: 1px solid #e0e0e0;
        }

        .react-af-querybox .react-af-autosuggest-facets-list {
          list-style: none;
          padding: 0;
          margin: 0;
        }

        .react-af-querybox .react-af-autosuggest-facet-item {
          padding: 10px 16px;
          cursor: pointer;
          border-bottom: 1px solid #f0f0f0;
          transition: all 0.2s;
          font-size: 14px;
        }

        .react-af-querybox .react-af-autosuggest-facet-item:hover,
        .react-af-querybox .react-af-autosuggest-facet-item.react-af-autosuggest-item-selected {
          background: #e8f4ff;
        }

        .react-af-querybox .react-af-autosuggest-facet-term {
          display: flex;
          justify-content: space-between;
          color: #333;
        }

        .react-af-querybox .react-af-autosuggest-facet-count {
          color: #999;
          font-size: 12px;
          background: #fff;
          padding: 2px 6px;
          border-radius: 10px;
          margin-left: 8px;
        }

        .answer-autosuggest-item {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }
        .answer-autosuggest-item strong {
          color: #333;
          font-size: 14px;
        }
        .answer-autosuggest-item small {
          color: #666;
          font-size: 12px;
        }
      `}</style>

      <div className="answer-container">
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
                <div className="answer-autosuggest-item">
                  <strong>{String(hit._source?.TICO || "")}</strong>
                  <small>{String(hit._source?.AUTR || "")}</small>
                </div>
              )}
            />
          </Autosuggest>
        </QueryBox>

        <div style={{ marginTop: "20px" }}>
          <AnswerResults
            id="answer"
            searchBoxId="question"
            generator={mockGenerator}
            showClassification={true}
            showReasoning={true}
            showFollowUpQuestions={true}
            showHits={true}
            renderClassification={(data) => (
              <div className="answer-classification">
                <strong>🎯 Classification:</strong> {data.route_type}{" "}
                <span style={{ color: "#666" }}>
                  ({(data.confidence * 100).toFixed(0)}% confident)
                </span>
                <div style={{ marginTop: "8px", fontSize: "13px", color: "#666" }}>
                  <div>
                    <strong>Query:</strong> {data.improved_query}
                  </div>
                  <div>
                    <strong>Semantic:</strong> {data.semantic_query}
                  </div>
                </div>
              </div>
            )}
            renderReasoning={(reasoning, isStreaming) => {
              // Convert [doc_id X] citations to markdown links
              const reasoningWithLinks = reasoning
                ? replaceCitations(reasoning, {
                    renderCitation: renderAsMarkdownLinks,
                  })
                : "";

              // Handle click events on citation links
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
                      element.style.background = "#f0f3ff";
                      setTimeout(() => {
                        element.style.background = "";
                      }, 2000);
                    }
                  }
                }
              };

              return (
                <div className="answer-reasoning">
                  <strong>🧠 Reasoning</strong>
                  {isStreaming && <span style={{ color: "#764ba2" }}> (streaming...)</span>}
                  <div
                    role="button"
                    tabIndex={0}
                    style={{ marginTop: "8px", fontSize: "14px", lineHeight: "1.6" }}
                    onClick={handleCitationClick}
                    onKeyDown={(e) =>
                      e.key === "Enter" &&
                      handleCitationClick(e as unknown as React.MouseEvent<HTMLDivElement>)
                    }
                  >
                    <Streamdown isAnimating={isStreaming}>{reasoningWithLinks}</Streamdown>
                  </div>
                </div>
              );
            }}
            renderAnswer={(answer, isStreaming, hits) => {
              // Convert [doc_id X] citations to markdown links
              const answerWithLinks = answer
                ? replaceCitations(answer, {
                    renderCitation: renderAsMarkdownLinks,
                  })
                : "";

              // Handle click events on citation links
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
                      element.style.background = "#f0f3ff";
                      setTimeout(() => {
                        element.style.background = "";
                      }, 2000);
                    }
                  }
                }
              };

              return (
                <div>
                  <div className="answer-section">
                    {!answer && !isStreaming && (
                      <div className="answer-empty">
                        No answer yet. Submit a question to get started.
                      </div>
                    )}
                    {answer && (
                      <div>
                        <div
                          role="button"
                          tabIndex={0}
                          className="answer-text"
                          onClick={handleCitationClick}
                          onKeyDown={(e) =>
                            e.key === "Enter" &&
                            handleCitationClick(e as unknown as React.MouseEvent<HTMLDivElement>)
                          }
                        >
                          <Streamdown isAnimating={isStreaming}>{answerWithLinks}</Streamdown>
                        </div>
                        {answerWithLinks.includes("[") && (
                          <div
                            style={{
                              marginTop: "12px",
                              fontSize: "14px",
                              color: "#666",
                              fontStyle: "italic",
                            }}
                          >
                            💡 Tip: Click citation links (e.g., [19], [16]) to jump to source
                            documents below (powered by{" "}
                            <a href="https://streamdown.ai/" target="_blank" rel="noopener">
                              Streamdown.ai
                            </a>
                            )
                          </div>
                        )}
                      </div>
                    )}
                  </div>

                  {hits &&
                    hits.length > 0 &&
                    (() => {
                      // Filter hits to only show cited documents
                      const citedIds = getCitedDocumentIds(answer);
                      const citedHits =
                        citedIds.length > 0
                          ? hits.filter((hit) => citedIds.includes(hit._id))
                          : hits;

                      return citedHits.length > 0 ? (
                        <div className="answer-hits-section">
                          <h2>
                            🔍 {citedIds.length > 0 ? "Cited" : "Source"} Documents (
                            {citedHits.length})
                          </h2>
                          {citedHits.map((hit) => (
                            <div key={hit._id} id={`hit-${hit._id}`} className="answer-hit">
                              <div className="answer-hit-header">
                                <h3 className="answer-hit-title">
                                  {String(hit._source?.TICO) || `Document ${hit._id}`}
                                </h3>
                                <span className="answer-hit-score">
                                  Score: {hit._score.toFixed(3)}
                                </span>
                              </div>
                              <div className="answer-hit-content">
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
                      ) : null;
                    })()}
                </div>
              );
            }}
            renderFollowUpQuestions={(questions) => {
              return (
                <div className="answer-followup">
                  <h3>❓ Suggested Follow-up Questions</h3>
                  <ul>
                    {questions.map((q) => {
                      // Convert [doc_id X] citations to markdown links
                      const questionWithLinks = replaceCitations(q, {
                        renderCitation: renderAsMarkdownLinks,
                      });
                      return (
                        <li key={q}>
                          <Streamdown>{questionWithLinks}</Streamdown>
                        </li>
                      );
                    })}
                  </ul>
                </div>
              );
            }}
            renderHits={(hits) => (
              <div className="answer-hits-section">
                <h2>📚 All Source Documents ({hits.length})</h2>
                {hits.map((hit) => (
                  <div key={hit._id} id={`all-hit-${hit._id}`} className="answer-hit">
                    <div className="answer-hit-header">
                      <h3 className="answer-hit-title">
                        {String(hit._source?.TICO) || `Document ${hit._id}`}
                      </h3>
                      <span className="answer-hit-score">Score: {hit._score.toFixed(3)}</span>
                    </div>
                    <div className="answer-hit-content">
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
            )}
          >
            <AnswerFeedback
              scale={1}
              renderRating={renderThumbsUpDown}
              onFeedback={handleFeedback}
              renderComment={(comment, setComment) => (
                <textarea
                  className="react-af-feedback-comment-input"
                  placeholder="Tell us what you think..."
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                />
              )}
              renderSubmit={(onSubmit) => (
                <button type="button" className="react-af-feedback-submit" onClick={onSubmit}>
                  Submit Feedback
                </button>
              )}
            />
          </AnswerResults>
        </div>
      </div>
    </Antfly>
  );
};
