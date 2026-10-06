import type { ReactNode } from "react";

type Json = Record<string, unknown>;
const object = (value: unknown): Json =>
  value && typeof value === "object" && !Array.isArray(value) ? (value as Json) : {};
const array = (value: unknown): Json[] => (Array.isArray(value) ? value.map(object) : []);
const label = (value: unknown) => (typeof value === "string" ? value : "");
const score = (value: unknown) =>
  typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(1)}%` : "—";
// Preserve distinct, identical predictions without using positional React keys.
function keyed(items: Json[]) {
  const counts = new Map<string, number>();
  return items.map((item) => {
    const content = JSON.stringify(item),
      occurrence = counts.get(content) ?? 0;
    counts.set(content, occurrence + 1);
    return { item, key: `${content}:${occurrence}` };
  });
}
function utf16Offset(text: string, offset: unknown, unit: unknown): number | null {
  if (typeof offset !== "number" || !Number.isInteger(offset) || offset < 0) return null;
  try {
    if (unit === "utf8_bytes") {
      const bytes = new TextEncoder().encode(text);
      return offset <= bytes.length
        ? new TextDecoder("utf-8", { fatal: true }).decode(bytes.slice(0, offset)).length
        : null;
    }
    if (unit === "unicode_codepoints") {
      const points = [...text];
      return offset <= points.length ? points.slice(0, offset).join("").length : null;
    }
    return offset <= text.length ? offset : null;
  } catch {
    return null;
  }
}
function cell(value: unknown): string {
  if (Array.isArray(value)) return value.map(cell).join(", ");
  const item = object(value);
  if ("value" in item) return String(item.value);
  if ("single" in item) return cell(item.single);
  if ("list" in item) return cell(item.list);
  return typeof value === "string" || typeof value === "number"
    ? String(value)
    : (JSON.stringify(value) ?? "");
}
export function Results({ value, text }: { value: Json; text: string | null }) {
  const row = Array.isArray(value.data) ? object(value.data[0]) : value;
  const entities = array(row.entities),
    decisions = array(row.decisions),
    classes = array(row.classifications),
    relations = array(row.relations);
  const structures: Record<string, Json[]> = {};
  if (Array.isArray(row.structures)) {
    for (const group of array(row.structures))
      structures[label(group.name)] = array(group.instances).map((record) =>
        Object.fromEntries(array(record.fields).map((field) => [label(field.name), field.value]))
      );
  } else
    for (const [name, records] of Object.entries(object(row.structures)))
      structures[name] = array(records);
  const highlighted: ReactNode[] = [];
  if (text !== null && entities.length) {
    let at = 0;
    for (const entity of [...entities].sort((a, b) => Number(a.start) - Number(b.start))) {
      const start = utf16Offset(text, entity.start, row.offset_unit),
        end = utf16Offset(text, entity.end, row.offset_unit);
      if (start === null || end === null || start < at || end <= start) continue;
      highlighted.push(
        text.slice(at, start),
        <mark key={`${start}-${end}`} title={label(entity.label)}>
          {text.slice(start, end)}
          <small>{label(entity.label)}</small>
        </mark>
      );
      at = end;
    }
    highlighted.push(text.slice(at));
  }
  return (
    <>
      {keyed(decisions).map(({ item: decision, key }) => (
        <section className="decision" key={key} aria-label={`${label(decision.name)} decision`}>
          <h3>
            {label(decision.name)} · {label(decision.label)}
          </h3>
          <p>
            {label(decision.type)} · Confidence {score(decision.confidence)} (
            {label(decision.confidence_method).replaceAll("_", " ")})
          </p>
          {keyed(array(decision.probabilities)).map(({ item: probability, key }) => (
            <div className="score" key={key}>
              <span>{label(probability.label)}</span>
              <meter
                aria-label={`${label(probability.label)} probability`}
                min="0"
                max="1"
                value={Number(probability.probability)}
              />
              <span>{score(probability.probability)}</span>
            </div>
          ))}
          {typeof decision.expected_value === "number" && (
            <p>Expected level (zero-based): {decision.expected_value.toFixed(3)}</p>
          )}
          {typeof decision.true_probability === "number" && (
            <p>True probability: {score(decision.true_probability)}</p>
          )}
          {typeof decision.act_probability === "number" && (
            <p>Action probability: {score(decision.act_probability)} · No action is executed.</p>
          )}
        </section>
      ))}
      {highlighted.length > 1 && (
        <section className="highlighted" aria-label="Entity highlights">
          {highlighted}
        </section>
      )}
      {entities.length > 0 && (
        <div className="entity-list">
          {keyed(entities).map(({ item: entity, key }) => (
            <div key={key}>
              <strong>{label(entity.text)}</strong>
              <span>{label(entity.label)}</span>
              <small>{score(entity.score)}</small>
              {entity.attributes != null && (
                <details>
                  <summary>Attributes</summary>
                  <pre>{JSON.stringify(entity.attributes, null, 2)}</pre>
                </details>
              )}
            </div>
          ))}
        </div>
      )}
      {keyed(decisions.length ? [] : classes).map(({ item: entry, key }) => (
        <div className="score" key={key}>
          <span>
            {label(entry.name)} {label(entry.label)}
          </span>
          <meter
            aria-label={`${label(entry.label)} confidence`}
            min="0"
            max="1"
            value={typeof entry.score === "number" ? entry.score : 0}
          />
          <span>{score(entry.score)}</span>
        </div>
      ))}
      {Object.entries(structures).map(([name, records]) => {
        const fields = [...new Set(records.flatMap((record) => Object.keys(record)))];
        return (
          <div className="record-table" key={name}>
            <table>
              <caption>{name}</caption>
              <thead>
                <tr>
                  {fields.map((field) => (
                    <th key={field} scope="col">
                      {field}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {keyed(records).map(({ item: record, key }) => (
                  <tr key={key}>
                    {fields.map((field) => (
                      <td key={field}>{cell(record[field])}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        );
      })}
      {relations.length > 0 && (
        <div className="relation-list">
          {keyed(relations).map(({ item: relation, key }) => (
            <div key={key}>
              <strong>{label(object(relation.source ?? relation.head).text)}</strong>
              <span> → {label(relation.type ?? relation.label)} → </span>
              <strong>{label(object(relation.target ?? relation.tail).text)}</strong>
              <small>{score(relation.score)}</small>
            </div>
          ))}
        </div>
      )}
      {!entities.length &&
        !decisions.length &&
        !classes.length &&
        !relations.length &&
        !Object.keys(structures).length && (
          <p className="note">No results matched this request and threshold.</p>
        )}
    </>
  );
}
