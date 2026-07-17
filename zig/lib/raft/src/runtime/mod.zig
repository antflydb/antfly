// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub const transport_iface = @import("transport_iface.zig");
pub const codec_iface = @import("codec_iface.zig");
pub const frame_driver_iface = @import("frame_driver_iface.zig");
pub const snapshot_transport_iface = @import("snapshot_transport_iface.zig");
pub const snapshot_iface = @import("snapshot_iface.zig");
pub const storage_iface = @import("storage_iface.zig");
pub const backpressure_iface = @import("backpressure_iface.zig");
pub const limit_backpressure = @import("limit_backpressure.zig");
pub const replica = @import("replica.zig");
pub const replica_catalog_iface = @import("replica_catalog_iface.zig");
pub const group = @import("group.zig");
pub const scheduler = @import("scheduler.zig");
pub const multi_raft = @import("multi_raft.zig");
pub const control_plane = @import("control_plane.zig");
pub const memory_batcher = @import("memory_batcher.zig");
pub const apply_worker = @import("apply_worker.zig");
pub const memory_replica_catalog = @import("memory_replica_catalog.zig");
pub const memory_replica_factory = @import("memory_replica_factory.zig");
pub const file_replica_catalog = @import("file_replica_catalog.zig");
pub const memory_transport = @import("memory_transport.zig");
pub const codec_transport = @import("codec_transport.zig");
pub const reconciler = @import("reconciler.zig");
pub const debug_codec = @import("debug_codec.zig");
pub const binary_codec = @import("binary_codec.zig");
pub const local_snapshot_transport = @import("local_snapshot_transport.zig");

pub const RuntimeConfig = multi_raft.RuntimeConfig;
pub const VirtualTime = scheduler.VirtualTime;
pub const GroupConfig = group.GroupConfig;
pub const MultiRaft = multi_raft.MultiRaft;
pub const HostMetrics = multi_raft.HostMetrics;
pub const HostCommand = control_plane.HostCommand;
pub const ReplicaDescriptor = replica.ReplicaDescriptor;
pub const ReplicaBootstrap = replica.ReplicaBootstrap;
pub const ReplicaRecord = replica.ReplicaRecord;
pub const EnsureReplicaResult = replica.EnsureReplicaResult;
pub const ReplicaCatalog = replica_catalog_iface.ReplicaCatalog;
pub const ReplicaFactory = replica_catalog_iface.ReplicaFactory;
pub const Backpressure = backpressure_iface.Backpressure;
pub const ReadyPressure = backpressure_iface.ReadyPressure;
pub const LimitBackpressure = limit_backpressure.LimitBackpressure;
pub const BackpressureLimits = limit_backpressure.Limits;
pub const DiskBatcher = storage_iface.DiskBatcher;
pub const PersistBatch = storage_iface.PersistBatch;
pub const ApplyQueue = storage_iface.ApplyQueue;
pub const InMemoryDiskBatcher = memory_batcher.InMemoryDiskBatcher;
pub const QueuedApplyWorker = apply_worker.QueuedApplyWorker;
pub const MemoryReplicaCatalog = memory_replica_catalog.MemoryReplicaCatalog;
pub const MemoryReplicaFactory = memory_replica_factory.MemoryReplicaFactory;
pub const FileReplicaCatalog = file_replica_catalog.FileReplicaCatalog;
pub const MessageCodec = codec_iface.MessageCodec;
pub const FrameDriver = frame_driver_iface.FrameDriver;
pub const SnapshotTransport = snapshot_transport_iface.SnapshotTransport;
pub const InMemoryTransportHost = memory_transport.InMemoryTransportHost;
pub const CodecTransportHost = codec_transport.CodecTransportHost;
pub const TransportRetryPolicy = codec_transport.RetryPolicy;
pub const PlacementProvider = reconciler.PlacementProvider;
pub const MemoryPlacementProvider = reconciler.MemoryPlacementProvider;
pub const PlacementIntent = reconciler.PlacementIntent;
pub const ReplicaReconciler = reconciler.ReplicaReconciler;
pub const DebugCodec = debug_codec.DebugCodec;
pub const BinaryCodec = binary_codec.BinaryCodec;
pub const LocalSnapshotTransport = local_snapshot_transport.LocalSnapshotTransport;

test "runtime module compiles" {
    _ = RuntimeConfig;
    _ = VirtualTime;
    _ = GroupConfig;
    _ = MultiRaft;
    _ = HostMetrics;
    _ = HostCommand;
    _ = ReplicaDescriptor;
    _ = ReplicaBootstrap;
    _ = ReplicaRecord;
    _ = EnsureReplicaResult;
    _ = ReplicaCatalog;
    _ = ReplicaFactory;
    _ = Backpressure;
    _ = ReadyPressure;
    _ = LimitBackpressure;
    _ = BackpressureLimits;
    _ = DiskBatcher;
    _ = PersistBatch;
    _ = ApplyQueue;
    _ = InMemoryDiskBatcher;
    _ = QueuedApplyWorker;
    _ = MemoryReplicaCatalog;
    _ = MemoryReplicaFactory;
    _ = FileReplicaCatalog;
    _ = MessageCodec;
    _ = FrameDriver;
    _ = SnapshotTransport;
    _ = InMemoryTransportHost;
    _ = CodecTransportHost;
    _ = TransportRetryPolicy;
    _ = PlacementProvider;
    _ = MemoryPlacementProvider;
    _ = PlacementIntent;
    _ = ReplicaReconciler;
    _ = DebugCodec;
    _ = BinaryCodec;
    _ = LocalSnapshotTransport;
}

test {
    _ = @import("runtime_test.zig");
    _ = @import("transport_test.zig");
    _ = @import("reconciler_test.zig");
}
