import type { Meta, StoryObj } from "@storybook/react-vite";
import React from "react";
import { FormProvider, useForm } from "react-hook-form";
import { fn } from "storybook/test";
import SchemaFieldRow from "../components/schema-builder/SchemaFieldRow";
import { Table, TableBody, TableHead, TableHeader, TableRow } from "../components/ui/table";

const meta: Meta<typeof SchemaFieldRow> = {
  title: "SchemaBuilder/SchemaFieldRow",
  component: SchemaFieldRow,
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
              properties: [
                {
                  name: "product_id",
                  type: "string",
                  description: "Unique identifier",
                  "x-antfly-index": true,
                  "x-antfly-types": ["keyword"],
                },
                {
                  name: "name",
                  type: "string",
                  description: "Product name",
                  "x-antfly-index": true,
                  "x-antfly-types": ["text", "keyword"],
                },
              ],
            },
          ],
        },
      });
      return (
        <FormProvider {...methods}>
          <div className="w-[800px]">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[28px]" />
                  <TableHead>Field</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Seen</TableHead>
                  <TableHead>Antfly Types</TableHead>
                  <TableHead className="w-[40px]" />
                </TableRow>
              </TableHeader>
              <TableBody>
                <Story />
              </TableBody>
            </Table>
          </div>
        </FormProvider>
      );
    },
  ],
  args: {
    onRemove: fn(),
    onToggleExpand: fn(),
    isExpanded: false,
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

export const Collapsed: Story = {
  args: {
    schemaIndex: 0,
    fieldIndex: 0,
  },
};

export const Expanded: Story = {
  args: {
    schemaIndex: 0,
    fieldIndex: 0,
    isExpanded: true,
  },
};

export const WithDetectionInfo: Story = {
  args: {
    schemaIndex: 0,
    fieldIndex: 1,
    isExpanded: true,
    detectionInfo: {
      frequency: 0.96,
      sampleCount: 50,
      exampleValue: "Wireless Bluetooth Headphones",
    },
  },
};

export const WithError: Story = {
  decorators: [
    (Story) => {
      const methods = useForm({
        mode: "onChange",
        defaultValues: {
          document_schemas: [
            {
              properties: [
                {
                  name: "_id",
                  type: "string",
                  description: "Unique identifier",
                  "x-antfly-index": true,
                  "x-antfly-types": [],
                },
              ],
            },
          ],
        },
      });
      React.useEffect(() => {
        methods.trigger();
      }, [methods]);
      return (
        <FormProvider {...methods}>
          <div className="w-[800px]">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[28px]" />
                  <TableHead>Field</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Seen</TableHead>
                  <TableHead>Antfly Types</TableHead>
                  <TableHead className="w-[40px]" />
                </TableRow>
              </TableHeader>
              <TableBody>
                <Story />
              </TableBody>
            </Table>
          </div>
        </FormProvider>
      );
    },
  ],
  args: {
    schemaIndex: 0,
    fieldIndex: 0,
    isExpanded: true,
  },
};
