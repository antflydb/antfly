"use client";

import { type ColumnDef, DataTable } from "@antfly/design-system";

type ClusterRow = { name: string; region: string; shards: number; qps: number };

const rows: ClusterRow[] = [
  { name: "prod-east", region: "us-east-1", shards: 3, qps: 1248 },
  { name: "prod-eu", region: "eu-central-1", shards: 2, qps: 412 },
  { name: "staging-east", region: "us-east-1", shards: 1, qps: 84 },
  { name: "dev-local", region: "us-west-2", shards: 1, qps: 3 },
];

const columns: ColumnDef<ClusterRow>[] = [
  { accessorKey: "name", header: "Cluster" },
  { accessorKey: "region", header: "Region" },
  { accessorKey: "shards", header: "Shards" },
  {
    accessorKey: "qps",
    header: () => <div className="text-right">QPS</div>,
    cell: ({ row }) => (
      <div className="text-right font-mono text-sm">
        {(row.getValue("qps") as number).toLocaleString()}
      </div>
    ),
  },
];

export function ClusterTableDemo() {
  return (
    <div className="max-w-3xl">
      <DataTable
        columns={columns}
        data={rows}
        filterColumn="name"
        filterPlaceholder="Filter clusters by name…"
        pageSize={3}
      />
    </div>
  );
}
