# Antfly Scaling and Node Shutdown

This document describes the long-term control-plane contract for adding and
removing Antfly data capacity. The goal is to make Kubernetes scale-down safe:
the operator must not terminate a pod until metadata says the node no longer
owns placement or runtime raft state.

## Model

Antfly has two related identities:

- **Node**: a process/pod identity. The Kubernetes operator scales nodes.
- **Store**: the placement/resource state hosted by a node. The production
  operator treats data nodes as one node with one store.

Node registration is the placement source of truth:

```http
POST /internal/v1/nodes
```

The request upserts node metadata and, for data nodes, the hosted store metadata
in the same payload. Production data-node registration includes `node_id`,
`store_id`, role, liveness, capacity, and current health. `store_id` must equal
`node_id`: node is the external lifecycle identity, and store is the internal
placement/resource record for that node. Metadata derives placement candidates
from registered nodes/stores that are live and not administratively draining.

Node status uses the same node resource:

```http
POST /internal/v1/nodes/{node_id}/status
```

`NodeRecord.lifecycle` is durable metadata. The current lifecycle values are
`active` and `draining`. A normal node registration defaults to `active`, but it
must not clear an existing non-active lifecycle. This makes shutdown intent
survive process restart and a later self-registration.

Registration must not be used to introduce `draining`; callers request draining
through `PUT /internal/v1/nodes/{node_id}/shutdown` so node lifecycle and hosted
store drain intent change together.

The first production use case is node shutdown for permanent scale-down:

```http
PUT /internal/v1/nodes/{node_id}/shutdown
GET /internal/v1/nodes/{node_id}/shutdown
DELETE /internal/v1/nodes/{node_id}/shutdown
DELETE /internal/v1/nodes/{node_id}
```

These endpoints are required runtime API, not Kubernetes-operator-only
conventions. A metadata server that supports node shutdown must implement all
four paths with the same semantics so either the Go or Zig operator/runtime can
drive scale-down safely.

## Shutdown Lifecycle

The shutdown API is node-oriented because operators remove pods/ordinals, not
individual stores. A shutdown request marks every store on the node as
administratively draining, marks the node lifecycle as `draining`, and triggers
reconciliation. If the node has no current stores, the node-level lifecycle still
persists so future node registration for that node inherits drain intent.

```text
active -> draining -> complete -> removed
       \-> active
```

The request is idempotent:

```json
{
  "type": "remove",
  "reason": "operator scale-down"
}
```

The status response is intentionally verbose so an operator can surface useful
conditions:

```json
{
  "node_id": 4,
  "type": "remove",
  "phase": "draining",
  "safe_to_terminate": false,
  "stores": [
    {
      "store_id": 4,
      "placement_intent_count": 2,
      "group_status_count": 2,
      "runtime_group_count": 2,
      "local_voter_count": 2,
      "local_leader_count": 0
    }
  ],
  "pending_groups": [1201, 1304]
}
```

`phase` is one of:

- `active`: the node is known and has no administrative drain intent.
- `draining`: the node is administratively draining and still has termination
  debt.
- `blocked`: the node cannot be safely drained until placement or raft safety
  changes. Metadata should include `blocked=true`, `blocked_reason`, and a
  human-readable `message`.
- `complete`: the node is still known, but has no remaining termination debt.
- `not_found`: metadata no longer has node, store, placement, or runtime state
  for the node.

`safe_to_terminate` is true when the node has reached a terminal shutdown state:
`phase=complete` for a known draining node, or `phase=not_found` for an already
removed node. For known nodes, `safe_to_terminate` is true only when the node
has:

- no placement intents,
- no reported local group status of any kind,
- no runtime group status.

Metadata must surface non-transient safety blocks instead of leaving the node in
`draining` forever. For example, if removing a local voter would leave a group
with no voters, the response should use `phase=blocked`,
`blocked_reason=InsufficientShardVoters`, and `safe_to_terminate=false`.

Healthy node registration and healthy node status reports must not clear
administrative drain intent. Runtime health and operator lifecycle intent are
separate inputs.

## Shutdown Cancellation

Shutdown is cancellable while the node is still desired by the control plane.
This covers the common operator case where a user requests scale-down, the node
enters `draining`, then the user scales back up before the pod is terminated.

Cancellation uses the same node-oriented resource:

```http
DELETE /internal/v1/nodes/{node_id}/shutdown
```

The request is idempotent. If the node is currently `draining`, metadata clears
the administrative drain intent for the node and for stores hosted by that node,
then triggers reconciliation. Future node registration and status for that node
is allowed to become `active` again.

Cancellation is not a rollback of raft or placement work that already completed
while the node was draining. If some groups were moved away before cancellation,
normal reconciliation may later add capacity back when the store is eligible
again. The only guarantee is that metadata stops treating the node as
administratively draining.

If `safe_to_terminate=true` has already been observed and the operator has
reduced StatefulSet replicas, cancellation should not resurrect that ordinal.
The next scale-up should start a fresh pod and let it register normally. A
metadata implementation may return `409 Conflict` for cancellation after the
node reaches `complete` or `removed`; treating late cancellation as a no-op is
also acceptable if the response clearly reports that the node is no longer
draining.

## Operator Scale-Down Protocol

For a scale-down from `N` to `N-1`:

1. Pick the highest StatefulSet ordinal that Kubernetes will remove.
2. Map the ordinal to the node ID used by the pod.
3. Call `PUT /internal/v1/nodes/{node_id}/shutdown`.
4. Requeue without shrinking the StatefulSet.
5. Poll `GET /internal/v1/nodes/{node_id}/shutdown`.
6. Only set StatefulSet replicas to `N-1` after `safe_to_terminate=true`.

Scale-up remains simple: start the pod and let it register with
`POST /internal/v1/nodes`.

If desired replicas increase while an ordinal is still in `draining` and before
the StatefulSet has been reduced, the operator should cancel the shutdown before
considering the scale-up complete:

1. Detect that the draining ordinal is desired again.
2. Call `DELETE /internal/v1/nodes/{node_id}/shutdown`.
3. Requeue and wait for the node shutdown status to report non-draining state,
   or for healthy registration/status to confirm the node is active.
4. Clear any scale-down condition on the custom resource.

If the StatefulSet was already reduced, the operator should use normal scale-up
instead of cancellation.

## Shutdown Finalization

`safe_to_terminate=true` means the runtime has removed placement and raft
dependency on the node. It does not mean Kubernetes has already terminated the
pod. Metadata should keep the drain record and store status until the operator
confirms the StatefulSet no longer includes the ordinal.

Once the StatefulSet has actually removed the ordinal, the operator finalizes
the shutdown:

```http
DELETE /internal/v1/nodes/{node_id}
```

Finalization is idempotent for already-removed nodes. For known nodes, metadata
must only finalize an existing shutdown intent: a live `active` node must be
rejected rather than deleted. Successful finalization removes:

- the node record,
- node-level drain intent,
- all store records hosted by that node,
- all store-level drain state for that node.

This keeps cancellation possible until Kubernetes has crossed the point of no
return, while still allowing a future scale-up of the same ordinal to register
as a fresh active node.

## Future Batch Planning

Batch topology changes can be layered above the node shutdown primitive later:

```http
POST /internal/v1/topology-plans
GET /internal/v1/topology-plans/{plan_id}
```

The first implementation should keep add and remove operations separate because
scale-up is immediate and scale-down is asynchronous.
