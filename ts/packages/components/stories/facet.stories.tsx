import { useState } from "react";
import { Antfly, Facet, type FacetProps, QueryBox, Results } from "../src";
import { tableName, url } from "./utils";

export default {
  title: "Facet",
  component: Facet,
};

function CollapsableFacet({ title, ...rest }: { title: string } & FacetProps) {
  const [collapsed, setCollapsed] = useState(true);

  function FacetWrapper() {
    if (!collapsed) {
      return <Facet {...rest} />;
    }
    return <div />;
  }
  return (
    <div>
      <div>
        {title}
        <button
          type="button"
          onClick={() => {
            setCollapsed(!collapsed);
          }}
        >
          open
        </button>
      </div>
      {FacetWrapper()}
    </div>
  );
}

export const Collapsable = () => {
  return (
    <Antfly url={url} table={tableName}>
      <QueryBox id="main" />
      <CollapsableFacet title="Author" id="autr" fields={["AUTR"]} />
      <Results
        id="result"
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

export const Customized = () => {
  return (
    <Antfly url={url} table={tableName}>
      <Facet
        seeMore="SEE MORE CUSTOM"
        placeholder="MY PLACEHOLDER"
        id="autr"
        fields={["AUTR"]}
        itemsPerBlock={10}
      />
      <Results
        id="result"
        items={(data) =>
          data.map(({ _source, _id, _score }) => (
            <div key={_id}>
              {String(_source?.TICO)} - score: {_score}
            </div>
          ))
        }
      />
    </Antfly>
  );
};

export const ModifyFilterValue = () => {
  return (
    <Antfly url={url} table={tableName}>
      <Facet
        filterValueModifier={(v) => `${v}.*`}
        placeholder="type first letters"
        id="autr"
        fields={["AUTR"]}
      />
      <Results
        id="result"
        items={(data) =>
          data.map(({ _source, _id, _score }) => (
            <div key={_id}>
              {String(_source?.TICO)} - score: {_score}
            </div>
          ))
        }
      />
    </Antfly>
  );
};

export const FacetWithCustomRenderItems = () => {
  return (
    <Antfly url={url} table={tableName}>
      <QueryBox id="main" />
      <Facet
        id="autr"
        fields={["AUTR"]}
        items={(data, { handleChange, isChecked }) => {
          return data.map((item) => (
            <label key={item.key} style={{ display: "block" }}>
              <input
                type="checkbox"
                checked={isChecked(item)}
                onChange={(e) => handleChange(item, e.target.checked)}
                style={{ marginRight: "8px" }}
              />
              <span style={{ color: isChecked(item) ? "green" : "black" }}>
                -`{">"}` {item.key}
              </span>
            </label>
          ));
        }}
      />
      <Results
        id="result"
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
