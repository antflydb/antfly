import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
  stories: ["../stories/**/*.stories.tsx"],
  addons: ["@storybook/addon-links", "@storybook/addon-docs"],
  features: {},
  framework: {
    name: "@storybook/react-vite",
    options: {},
  },
};

export default config;
