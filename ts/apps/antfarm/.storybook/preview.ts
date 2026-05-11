import type { GeneratorConfig, TableStatus } from "@antfly/sdk";
import type { Preview } from "@storybook/react-vite";
import React from "react";
import { ApiConfigProvider } from "../src/components/api-config-provider";
import { ThemeProvider } from "../src/components/theme-provider";
import { GeneratorPreferenceContext } from "../src/contexts/generator-preference-context";
import { TableContext } from "../src/contexts/table-context";
import "../src/global.css";

const storybookTables: TableStatus[] = [
  {
    name: "storybook_table",
    status: "ready",
  } as TableStatus,
];

const preview: Preview = {
  decorators: [
    (Story) => {
      const [dashboardGenerator, setDashboardGenerator] = React.useState<GeneratorConfig | null>(
        null
      );
      const [selectedTable, setSelectedTable] = React.useState("storybook_table");
      const [selectedIndex, setSelectedIndex] = React.useState("storybook_embedding");

      return React.createElement(
        ThemeProvider,
        null,
        React.createElement(
          ApiConfigProvider,
          null,
          React.createElement(
            GeneratorPreferenceContext.Provider,
            {
              value: {
                dashboardGenerator,
                setDashboardGenerator,
              },
            },
            React.createElement(
              TableContext.Provider,
              {
                value: {
                  tables: storybookTables,
                  isLoadingTables: false,
                  selectedTable,
                  setSelectedTable,
                  embeddingIndexes: ["storybook_embedding"],
                  chatIndexes: ["storybook_embedding", "storybook_full_text"],
                  isLoadingIndexes: false,
                  selectedIndex,
                  setSelectedIndex,
                  refreshTables: async () => undefined,
                },
              },
              React.createElement(
                "div",
                { className: "af-dashboard p-4" },
                React.createElement(Story)
              )
            )
          )
        )
      );
    },
  ],
  parameters: {
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },

    a11y: {
      // 'todo' - show a11y violations in the test UI only
      // 'error' - fail CI on a11y violations
      // 'off' - skip a11y checks entirely
      test: "todo",
    },
  },
};

export default preview;
