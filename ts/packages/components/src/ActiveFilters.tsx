import type { ReactNode } from "react";
import { useSharedContext } from "./SharedContext";

export interface ActiveFilter {
  key: string;
  value: string;
}

export interface ActiveFiltersProps {
  items?: (activeFilters: ActiveFilter[], removeFilter: (id: string) => void) => ReactNode;
}

export default function ActiveFilters({ items }: ActiveFiltersProps) {
  const [{ widgets }, dispatch] = useSharedContext();
  const activeFilters: ActiveFilter[] = [...widgets]
    .filter(([, v]) => (Array.isArray(v.value) ? v.value.length : v.value))
    .map(([k, v]) => ({
      key: k,
      value: Array.isArray(v.value) ? v.value.join(", ") : String(v.value),
    }));

  // On filter remove, update widget properties.
  function removeFilter(id: string) {
    const widget = widgets.get(id);
    if (widget) {
      dispatch({
        type: "setWidget",
        key: id,
        ...widget,
        value: widget.isFacet ? [] : "",
      });
    }
  }

  return (
    <div className="react-af-active-filters">
      {items ? (
        items(activeFilters, removeFilter)
      ) : (
        <ul>
          {activeFilters.map(({ key, value }) => {
            return (
              <li key={key}>
                <span>{`${key}: ${value}`}</span>
                <button type="button" onClick={() => removeFilter(key)}>
                  x
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
