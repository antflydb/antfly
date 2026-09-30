# Physical store-root enrollment

Hosted initial foreign-key publication and replica retirement bind owner
receipts to an administrator-approved physical store root. Ordinary node
registration advertises the root's public key; it does not approve that key.
Enroll each participating data store before using these workflows.

## Enroll a store

1. Start the data store so that its durable root identity exists and its current
   registration is visible. Obtain the target metadata cluster's canonical
   32-character hexadecimal incarnation from its administrative status.
2. On that data node, generate a proof using the existing replica-root directory:

   ```sh
   antfly internal store-root proof \
     --replica-root-dir /absolute/path/to/replicas \
     --metadata-incarnation METADATA_CLUSTER_INCARNATION \
     --node-id NODE_ID --store-id STORE_ID > store-root-proof.json
   ```

   This reads the local signing checkpoint and emits only the public identity
   and signature. It neither creates a replacement identity nor exports the
   private seed. Inspect the node, store, cluster and root identity before
   approving the proof.
3. With the CLI configured for the target API and a cluster-administrator
   credential, submit the proof once:

   ```sh
   antfly internal store-root enroll --file store-root-proof.json
   ```

   The equivalent public operation is `POST /db/v1/store-roots/enroll`.
   Enrollment is immutable for the exact store/root identity. Replacing a disk
   or changing its signing key requires explicit enrollment of the new root;
   possession of a service credential is insufficient.
4. If the response is lost or reports an uncertain outcome, check the same
   identity without submitting another mutation:

   ```sh
   antfly internal store-root status --file store-root-proof.json
   ```

   This uses the read-only `POST /db/v1/store-roots/enrollment-status` operation.
   A successful response confirms the exact enrollment, not an enrollment for
   another root on the same node. An unavailable metadata quorum is not proof
   that the original enrollment failed; retry the status read after recovery.
   A `409` means the exact identity is not enrolled or its registration changed;
   investigate the current registration before approving a replacement.

## Retirement and recovery

Retirement is authorized by metadata work for an exact plan, replica and root
generation, not by a directory name or a shared service secret. The data owner
checks the cold bootstrap and local replica catalog, durably records its intent,
quiesces the replica, and moves the exact root to retirement trash before
acknowledging completion. Restart resumes the intent. A permanent generation
fence prevents stale placement from reopening that retired replica.

Trash reclamation is separate from logical retirement. It advances in bounded
slices, does not follow symbolic links, and preserves the permanent fence after
the files are gone. An offline store can finish its own outstanding work after
returning; other stores cannot acknowledge deletion on its behalf. Do not
manually remove retirement intents or generation fences to force progress.

These signatures attest to possession of a durable root identity, not to a
unique physical device or hardware-backed erasure. A byte-for-byte copy that
includes the private signing checkpoint shares that identity. Portable backups
must not export the checkpoint; restore into a different physical root uses
its own identity and enrollment.
