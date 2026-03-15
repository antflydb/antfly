import type { AggregationBucket } from "@antfly/sdk";
import { type ReactNode, useEffect, useState } from "react";
import { useSharedContext } from "./SharedContext";
import { disjunctsFrom, toTermQueries } from "./utils";

export interface FacetProps {
  fields: string[];
  id: string;
  initialValue?: string[];
  seeMore?: string;
  placeholder?: string;
  showFilter?: boolean;
  filterValueModifier?: (value: string) => string;
  itemsPerBlock?: number;
  table?: string; // Optional table override (Phase 1: single table only)
  items?: (
    data: AggregationBucket[],
    options: {
      handleChange: (item: AggregationBucket, checked: boolean) => void;
      isChecked: (item: AggregationBucket) => boolean;
    }
  ) => ReactNode;
}

export default function Facet({
  fields,
  id,
  initialValue,
  seeMore,
  placeholder,
  showFilter = true,
  filterValueModifier,
  itemsPerBlock,
  table,
  items,
}: FacetProps) {
  const [{ widgets }, dispatch] = useSharedContext();
  // Current filter (search inside facet value).
  const [filterValue, setFilterValue] = useState("");
  // Number of items displayed in facet.
  const [size, setSize] = useState(itemsPerBlock || 5);
  // The actual selected items in facet.
  const [value, setValue] = useState<string[]>(initialValue || []);
  // Data from internal queries (Antfly queries are performed via Listener)
  const widget = widgets.get(id);
  const { result } = widget || {};
  // Facet component always expects a single array, not array of arrays
  const rawFacetData = result?.facetData;
  const data: AggregationBucket[] =
    rawFacetData && Array.isArray(rawFacetData) && !Array.isArray(rawFacetData[0])
      ? (rawFacetData as AggregationBucket[])
      : [];

  // Update widgets properties on state change.
  useEffect(() => {
    dispatch({
      type: "setWidget",
      key: id,
      needsQuery: true,
      needsConfiguration: true,
      isFacet: true,
      wantResults: false,
      query: disjunctsFrom(toTermQueries(fields, value)),
      value,
      table: table,
      configuration: { size, filterValue, fields, filterValueModifier },
    });
  }, [dispatch, id, size, filterValue, value, fields, filterValueModifier, table]);

  // If widget value was updated elsewhere (ex: from active filters deletion)
  // We have to update and dispatch the component.
  // useEffect(() => {
  //   widgets.get(id) && setValue(widgets.get(id).value);
  // }, [isValueReady()]);
  //
  // The original useEffect with isValueReady() in the dependency array
  // was causing the effect to run on every render,  constantly resetting the
  // local state back to the widget's value and preventing the
  // checkbox selection from persisting.
  //
  // The new approach only syncs the local state when the widget's actual value
  // changes from external sources (like active filter removal), which is
  // what was intended.
  const widgetValue = widgets.get(id)?.value;
  useEffect(() => {
    if (widgetValue && Array.isArray(widgetValue) && widgetValue !== value) {
      setValue(widgetValue);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [widgetValue, value]); // Remove value from deps to prevent loops

  // Destroy widget from context (remove from the list to unapply its effects)
  useEffect(() => () => dispatch({ type: "deleteWidget", key: id }), [dispatch, id]);

  // On checkbox status change, add or remove current agg to selected
  function handleChange(item: AggregationBucket, checked: boolean) {
    const newValue = checked
      ? [...new Set([...value, item.key])]
      : value.filter((f) => f !== item.key);
    setValue(newValue);
  }

  // Is current item checked?
  function isChecked(item: AggregationBucket): boolean {
    return value.includes(item.key);
  }

  return (
    <div className="react-af-facet">
      {showFilter ? (
        <input
          value={filterValue}
          placeholder={placeholder || "filter…"}
          type="text"
          onChange={(e) => {
            setFilterValue(e.target.value);
          }}
        />
      ) : null}
      {items
        ? items(data, { handleChange, isChecked })
        : data.map((item) => (
            <label key={item.key}>
              <input
                type="checkbox"
                checked={isChecked(item)}
                onChange={(e) => handleChange(item, e.target.checked)}
              />
              {item.key} ({item.doc_count})
            </label>
          ))}
      {data.length === size ? (
        <button type="button" onClick={() => setSize(size + (itemsPerBlock || 5))}>
          {seeMore || "see more"}
        </button>
      ) : null}
    </div>
  );
}
