import type { GraphColorConfig } from "./types";

const THEMES = { light: "", dark: ".dark" } as const;

export function GraphStyle({
  id,
  colorConfig,
  typeOrder,
}: {
  id: string;
  colorConfig?: GraphColorConfig;
  typeOrder: string[];
}) {
  const entries = typeOrder
    .map((type) => {
      const cfg = colorConfig?.[type];
      if (!cfg?.theme && !cfg?.color) return null;
      return [type, cfg] as const;
    })
    .filter(Boolean) as [string, NonNullable<GraphColorConfig[string]>][];

  if (!entries.length) return null;

  return (
    <style
      // biome-ignore lint/security/noDangerouslySetInnerHtml: CSS variables are generated from typed color config, not user HTML.
      dangerouslySetInnerHTML={{
        __html: Object.entries(THEMES)
          .map(([theme, prefix]) => {
            const vars = entries
              .map(([type, cfg]) => {
                const color = cfg.theme?.[theme as keyof typeof cfg.theme] || cfg.color;
                return color ? `  --graph-color-${type}: ${color};` : null;
              })
              .filter(Boolean)
              .join("\n");
            return vars ? `${prefix} [data-graph="${id}"] {\n${vars}\n}` : "";
          })
          .filter(Boolean)
          .join("\n"),
      }}
    />
  );
}
