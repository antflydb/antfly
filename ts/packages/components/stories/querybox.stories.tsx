import {
  AnswerResults,
  Antfly,
  Autosuggest,
  Facet,
  type GeneratorConfig,
  QueryBox,
  Results,
} from "../src";
import { tableName, url } from "./utils";

// Mock generator configuration
const mockGenerator: GeneratorConfig = {
  provider: "ollama",
  model: "gemma3:4b",
};

export default {
  title: "QueryBox",
  component: QueryBox,
};

export const LiveMode = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>QueryBox in Live Mode (queries as you type)</h1>
      <pre>{`<QueryBox id="search" mode="live" />
<Results
  id="results"
  searchBoxId="search"
  fields={["AUTR", "TICO"]}
  items={(data) => ...}
/>`}</pre>
      <QueryBox id="search" mode="live" placeholder="Search..." />
      <Results
        id="results"
        searchBoxId="search"
        fields={["AUTR", "TICO"]}
        items={(data) =>
          data.map(({ _source: s, _id }) => (
            <div key={_id}>
              {String(s?.TICO)} - {String(s?.AUTR)}
            </div>
          ))
        }
        pagination={() => <></>}
      />
    </Antfly>
  );
};

export const SubmitMode = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>QueryBox in Submit Mode (queries on submit)</h1>
      <pre>{`<QueryBox id="search" mode="submit" buttonLabel="Search" />
<Results
  id="results"
  searchBoxId="search"
  fields={["AUTR"]}
  items={(data) => ...}
/>`}</pre>
      <QueryBox id="search" mode="submit" buttonLabel="Search" placeholder="Enter query..." />
      <Results
        id="results"
        searchBoxId="search"
        fields={["AUTR"]}
        items={(data) => data.map(({ _source: s, _id }) => <div key={_id}>{String(s?.TICO)}</div>)}
        pagination={() => <></>}
      />
    </Antfly>
  );
};

export const WithAutosuggest = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>QueryBox with Autosuggest</h1>
      <pre>{`<QueryBox id="search" mode="live">
  <Autosuggest fields={["AUTR"]} limit={5} minChars={2} />
</QueryBox>
<Results searchBoxId="search" fields={["AUTR"]} />`}</pre>
      <QueryBox id="search" mode="live">
        <Autosuggest fields={["AUTR"]} limit={5} minChars={2} />
      </QueryBox>
      <Results
        id="results"
        searchBoxId="search"
        fields={["AUTR"]}
        items={(data) =>
          data.map(({ _source: s, _id }) => (
            <div key={_id}>
              {String(s?.TICO)} - {String(s?.AUTR)}
            </div>
          ))
        }
        pagination={() => <></>}
      />
    </Antfly>
  );
};

export const WithFacets = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>QueryBox with Facets</h1>
      <pre>{`<QueryBox id="search" mode="live" />
<Facet id="domain" fields={["DOMA"]} />
<Results searchBoxId="search" fields={["AUTR", "TICO"]} />`}</pre>
      <div style={{ display: "flex", gap: "20px" }}>
        <div style={{ flex: 1 }}>
          <QueryBox id="search" mode="live" />
          <Results
            id="results"
            searchBoxId="search"
            fields={["AUTR", "TICO"]}
            items={(data) =>
              data.map(({ _source: s, _id }) => (
                <div key={_id}>
                  {String(s?.TICO)} - {String(s?.AUTR)}
                </div>
              ))
            }
            pagination={() => <></>}
          />
        </div>
        <div style={{ width: "200px" }}>
          <h3>Filter by Domain</h3>
          <Facet id="domain" fields={["DOMA"]} />
        </div>
      </div>
    </Antfly>
  );
};

export const RAGMode = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>QueryBox for RAG (Submit Mode)</h1>
      <pre>{`<QueryBox id="rag" mode="submit" buttonLabel="Ask" />
<AnswerResults
  id="rag-results"
  searchBoxId="rag"
  generator={{...}}
  fields={["TICO", "AUTR"]}
/>`}</pre>
      <QueryBox id="rag" mode="submit" buttonLabel="Ask" placeholder="Ask a question..." />
      <AnswerResults
        id="rag-results"
        searchBoxId="rag"
        generator={mockGenerator}
        fields={["TICO", "AUTR"]}
        showHits={true}
      />
    </Antfly>
  );
};

export const AnswerMode = () => {
  return (
    <Antfly url={url} table={tableName}>
      <h1>QueryBox for Answer Agent (Submit Mode)</h1>
      <pre>{`<QueryBox id="answer" mode="submit" buttonLabel="Ask" />
<AnswerResults
  id="answer-results"
  searchBoxId="answer"
  generator={{...}}
/>`}</pre>
      <QueryBox id="answer" mode="submit" buttonLabel="Ask" placeholder="Ask a question..." />
      <AnswerResults
        id="answer-results"
        searchBoxId="answer"
        generator={mockGenerator}
        showFollowUpQuestions={true}
        showHits={true}
      />
    </Antfly>
  );
};
