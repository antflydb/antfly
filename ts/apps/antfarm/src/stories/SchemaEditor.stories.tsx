import type { Meta, StoryObj } from "@storybook/react-vite";
import { FormProvider, useForm } from "react-hook-form";
import { fn } from "storybook/test";
import SchemaEditor from "../components/schema-builder/SchemaEditor";

const meta: Meta<typeof SchemaEditor> = {
  title: "SchemaBuilder/SchemaEditor",
  component: SchemaEditor,
  parameters: {
    layout: "centered",
  },
  tags: ["autodocs"],
  decorators: [
    (Story) => {
      const methods = useForm({
        defaultValues: {
          document_schemas: [
            {
              name: "products",
              key: "product_id",
              properties: [
                {
                  name: "product_id",
                  type: "string",
                  description: "Unique identifier",
                },
                { name: "name", type: "string", description: "Product name" },
                { name: "price", type: "number", description: "Product price" },
              ],
            },
          ],
        },
      });
      return (
        <FormProvider {...methods}>
          <div className="w-[800px]">
            <Story />
          </div>
        </FormProvider>
      );
    },
  ],
  args: {
    onRemove: fn(),
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    schemaIndex: 0,
  },
};

export const Empty: Story = {
  decorators: [
    (Story) => {
      const methods = useForm({
        defaultValues: {
          document_schemas: [
            {
              name: "",
              key: "",
              properties: [],
            },
          ],
        },
      });
      return (
        <FormProvider {...methods}>
          <div className="w-[800px]">
            <Story />
          </div>
        </FormProvider>
      );
    },
  ],
  args: {
    schemaIndex: 0,
  },
};
