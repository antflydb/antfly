// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package foreign

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/antflydb/antfly/lib/evaluator"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/store"
	"github.com/antflydb/antfly/src/store/db"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/zap"
)

// opDeleteDocument is the reserved operation name that triggers a full document deletion
// instead of field-level transforms during CDC delete processing.
const opDeleteDocument = "$delete_document"

// columnRefPattern matches {{column}} or {{column.key}} references in transform values.
var columnRefPattern = regexp.MustCompile(`\{\{(\w+(?:\.\w+)*)\}\}`)

// ReplicationConfig holds configuration for a single PostgreSQL CDC replication stream.
type ReplicationConfig struct {
	TableName         string                         // target Antfly table
	SlotName          string                         // PG replication slot (auto-derived or user-specified)
	PublicationName   string                         // PG publication (auto-derived or user-specified)
	DSN               string                         // resolved connection string
	PostgresTable     string                         // source PG table
	KeyTemplate       string                         // "id" or "{{tenant_id}}:{{user_id}}"
	OnUpdate          []store.ReplicationTransformOp // nil = auto $set all columns
	OnDelete          []store.ReplicationTransformOp // nil = auto $unset from on_update paths
	PublicationFilter json.RawMessage                // bleve filter → SQL WHERE on publication
	Routes            []store.ReplicationRouteConfig // conditional fan-out routes
}

// TransformFunc applies transform operations to an Antfly document.
type TransformFunc func(ctx context.Context, tableName, key string, ops []*db.TransformOp, upsert bool) error

// DeleteFunc deletes an Antfly document entirely.
type DeleteFunc func(ctx context.Context, tableName, key string) error

// LSNStore persists and loads replication LSN positions for crash recovery.
type LSNStore interface {
	LoadLSN(ctx context.Context, slotName string) (pglogrepl.LSN, error)
	SaveLSN(ctx context.Context, slotName string, lsn pglogrepl.LSN) error
}

// ReplicationWorker manages a single PostgreSQL logical replication stream,
// converting WAL changes into Antfly transform operations.
type ReplicationWorker struct {
	config        ReplicationConfig
	lsnStore      LSNStore
	transformFunc TransformFunc
	deleteFunc    DeleteFunc
	logger        *zap.Logger
	relations     map[uint32]*pglogrepl.RelationMessageV2 // OID -> column metadata cache
	typeMap       *pgtype.Map
	routeFilters  []evaluator.FilterNode // pre-compiled route Where filters, indexed parallel to config.Routes
}

// resolvedOp is an intermediate representation of a transform operation after
// column references have been resolved against the decoded PostgreSQL row.
type resolvedOp struct {
	Op    string
	Path  string
	Value any
}

// NewReplicationWorker creates a new ReplicationWorker for a single CDC stream.
func NewReplicationWorker(
	config ReplicationConfig,
	lsnStore LSNStore,
	transformFunc TransformFunc,
	deleteFunc DeleteFunc,
	logger *zap.Logger,
) *ReplicationWorker {
	return &ReplicationWorker{
		config:        config,
		lsnStore:      lsnStore,
		transformFunc: transformFunc,
		deleteFunc:    deleteFunc,
		logger:        logger.With(zap.String("slot", config.SlotName), zap.String("pg_table", config.PostgresTable)),
		relations:     make(map[uint32]*pglogrepl.RelationMessageV2),
		typeMap:       pgtype.NewMap(),
	}
}

// Run is the outer loop that reconnects with exponential backoff on transient errors.
// It stops on permanent errors (auth, missing table) or context cancellation.
func (w *ReplicationWorker) Run(ctx context.Context) error {
	// Pre-compile route filters once at startup to avoid per-row JSON parsing.
	w.routeFilters = make([]evaluator.FilterNode, len(w.config.Routes))
	for i := range w.config.Routes {
		node, err := evaluator.ParseFilter(w.config.Routes[i].Where)
		if err != nil {
			return fmt.Errorf("compiling route %s filter: %w", w.config.Routes[i].TargetTable, err)
		}
		w.routeFilters[i] = node
	}

	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		err := w.runOnce(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isPermanentReplicationError(err) {
			w.logger.Error("permanent replication error, stopping worker", zap.Error(err))
			return fmt.Errorf("permanent replication error: %w", err)
		}

		// Jittered exponential backoff
		jitter := time.Duration(rand.Int64N(int64(backoff) / 2)) //nolint:gosec // G404: non-security randomness for ML/jitter
		sleep := backoff + jitter
		w.logger.Warn("replication error, reconnecting", zap.Error(err), zap.Duration("backoff", sleep))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}

		backoff = min(backoff*2, maxBackoff)
	}
}

// runOnce runs a single replication session: connect, ensure publication/slot,
// load persisted LSN, start replication, and enter the receive loop.
func (w *ReplicationWorker) runOnce(ctx context.Context) error {
	// Append replication=database to the DSN for replication mode connections
	dsn := w.config.DSN
	if strings.Contains(dsn, "?") {
		dsn += "&replication=database"
	} else {
		dsn += "?replication=database"
	}

	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	w.logger.Info("connected to postgres for replication")

	if err := w.ensurePublication(ctx, conn); err != nil {
		return fmt.Errorf("ensuring publication: %w", err)
	}

	if err := w.ensureReplicationSlot(ctx, conn); err != nil {
		return fmt.Errorf("ensuring replication slot: %w", err)
	}

	// Load persisted LSN for resuming after restart
	startLSN, err := w.lsnStore.LoadLSN(ctx, w.config.SlotName)
	if err != nil {
		return fmt.Errorf("loading LSN: %w", err)
	}
	w.logger.Info("starting replication", zap.String("start_lsn", startLSN.String()))

	// Start logical replication with pgoutput v2 protocol
	err = pglogrepl.StartReplication(ctx, conn, w.config.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			"proto_version '2'",
			fmt.Sprintf("publication_names '%s'", w.config.PublicationName),
		},
	})
	if err != nil {
		return fmt.Errorf("starting replication: %w", err)
	}

	return w.receiveLoop(ctx, conn, startLSN)
}

// ensurePublication creates the publication if it does not already exist.
func (w *ReplicationWorker) ensurePublication(ctx context.Context, conn *pgconn.PgConn) error {
	// CREATE PUBLICATION does not support IF NOT EXISTS, so check first.
	checkSQL := fmt.Sprintf(
		"SELECT 1 FROM pg_publication WHERE pubname = %s",
		pgQuoteLiteral(w.config.PublicationName),
	)
	result := conn.Exec(ctx, checkSQL)
	results, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("check publication: %w", err)
	}

	// If publication already exists, nothing to do
	if len(results) > 0 && len(results[0].Rows) > 0 {
		w.logger.Debug("publication already exists", zap.String("publication", w.config.PublicationName))
		return nil
	}

	createSQL := fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s",
		pgQuoteIdentifier(w.config.PublicationName),
		pgQuoteIdentifier(w.config.PostgresTable),
	)

	// Append WHERE clause if publication_filter is set.
	if len(w.config.PublicationFilter) > 0 {
		whereSQL, err := FilterToLiteralSQL(w.config.PublicationFilter, nil)
		if err != nil {
			return fmt.Errorf("translating publication_filter to SQL: %w", err)
		}
		if whereSQL != "" {
			createSQL += " WHERE (" + whereSQL + ")"
		}
	}

	result = conn.Exec(ctx, createSQL)
	_, err = result.ReadAll()
	if err != nil {
		return fmt.Errorf("create publication: %w", err)
	}
	w.logger.Debug("publication created", zap.String("publication", w.config.PublicationName))
	return nil
}

// ensureReplicationSlot creates the replication slot if it does not already exist.
func (w *ReplicationWorker) ensureReplicationSlot(ctx context.Context, conn *pgconn.PgConn) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, conn, w.config.SlotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	})
	if err != nil {
		// Check if the slot already exists (PG error code 42710 = duplicate_object)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" {
			w.logger.Debug("replication slot already exists", zap.String("slot", w.config.SlotName))
			return nil
		}
		return fmt.Errorf("create replication slot: %w", err)
	}
	w.logger.Info("created replication slot", zap.String("slot", w.config.SlotName))
	return nil
}

// receiveLoop reads WAL messages, dispatches them, and sends standby status updates.
func (w *ReplicationWorker) receiveLoop(ctx context.Context, conn *pgconn.PgConn, startLSN pglogrepl.LSN) error {
	currentLSN := startLSN
	lastStatusTime := time.Now()
	standbyInterval := 10 * time.Second
	inStream := false

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Send standby status every 10s to prevent wal_sender_timeout
		if time.Since(lastStatusTime) >= standbyInterval {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: currentLSN,
			})
			if err != nil {
				return fmt.Errorf("sending standby status: %w", err)
			}
			lastStatusTime = time.Now()
		}

		// 3s deadline for receiving messages so we can send keepalive regularly
		receiveCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if receiveCtx.Err() != nil && ctx.Err() == nil {
				// Timeout on receive, loop to send standby status
				continue
			}
			return fmt.Errorf("receiving message: %w", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("parsing XLogData: %w", err)
				}

				logicalMsg, err := pglogrepl.ParseV2(xld.WALData, inStream)
				if err != nil {
					return fmt.Errorf("parsing logical message: %w", err)
				}

				if err := w.handleMessage(ctx, logicalMsg, &currentLSN, &inStream); err != nil {
					return fmt.Errorf("handling message: %w", err)
				}

				if xld.WALStart+pglogrepl.LSN(len(xld.WALData)) > currentLSN {
					currentLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				}

			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("parsing keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: currentLSN,
					})
					if err != nil {
						return fmt.Errorf("sending standby status (reply): %w", err)
					}
					lastStatusTime = time.Now()
				}
			}

		default:
			w.logger.Warn("unexpected message type in replication stream", zap.String("type", fmt.Sprintf("%T", rawMsg)))
		}
	}
}

// handleMessage dispatches a decoded pgoutput v2 message to the appropriate handler.
func (w *ReplicationWorker) handleMessage(ctx context.Context, msg pglogrepl.Message, currentLSN *pglogrepl.LSN, inStream *bool) error {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessageV2:
		w.relations[m.RelationID] = m

	case *pglogrepl.InsertMessageV2:
		rel, ok := w.relations[m.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID %d for insert", m.RelationID)
		}
		if err := w.processDataChange(ctx, rel, m.Tuple); err != nil {
			w.logger.Error("processing insert", zap.Error(err))
		}

	case *pglogrepl.UpdateMessageV2:
		rel, ok := w.relations[m.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID %d for update", m.RelationID)
		}
		if err := w.processDataChange(ctx, rel, m.NewTuple); err != nil {
			w.logger.Error("processing update", zap.Error(err))
		}

	case *pglogrepl.DeleteMessageV2:
		rel, ok := w.relations[m.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID %d for delete", m.RelationID)
		}
		if err := w.processDelete(ctx, rel, m.OldTuple); err != nil {
			w.logger.Error("processing delete", zap.Error(err))
		}

	case *pglogrepl.CommitMessage:
		*currentLSN = m.TransactionEndLSN
		if err := w.lsnStore.SaveLSN(ctx, w.config.SlotName, m.TransactionEndLSN); err != nil {
			w.logger.Error("saving LSN checkpoint", zap.Error(err))
		}

	case *pglogrepl.TruncateMessageV2:
		w.logger.Warn("received TRUNCATE event, skipping",
			zap.String("pg_table", w.config.PostgresTable))

	case *pglogrepl.BeginMessage:
		// Begin of transaction, nothing to do

	case *pglogrepl.StreamStartMessageV2:
		*inStream = true

	case *pglogrepl.StreamStopMessageV2:
		*inStream = false

	case *pglogrepl.StreamCommitMessageV2:
		*currentLSN = pglogrepl.LSN(m.TransactionEndLSN)
		if err := w.lsnStore.SaveLSN(ctx, w.config.SlotName, m.TransactionEndLSN); err != nil {
			w.logger.Error("saving LSN checkpoint (stream commit)", zap.Error(err))
		}

	case *pglogrepl.StreamAbortMessageV2:
		// Aborted streamed transaction, nothing to do

	case *pglogrepl.TypeMessageV2:
		// Type definition, nothing to do
	}

	return nil
}

// processDataChange handles INSERT and UPDATE events by resolving transforms
// and forwarding them to the Antfly table.
func (w *ReplicationWorker) processDataChange(ctx context.Context, rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData) error {
	row, err := tupleToMap(rel, tuple, w.typeMap)
	if err != nil {
		return fmt.Errorf("decoding tuple: %w", err)
	}

	if len(w.config.Routes) > 0 {
		return w.processDataChangeRouted(ctx, rel, row)
	}

	key, err := extractKey(row, w.config.KeyTemplate)
	if err != nil {
		return fmt.Errorf("extracting key: %w", err)
	}

	var resolved []resolvedOp
	if w.config.OnUpdate == nil {
		resolved = autoOnUpdate(rel, row)
	} else {
		resolved, err = resolveTransforms(w.config.OnUpdate, row)
		if err != nil {
			return fmt.Errorf("resolving on_update transforms: %w", err)
		}
	}

	ops, err := toDBTransformOps(resolved)
	if err != nil {
		return fmt.Errorf("converting to db transform ops: %w", err)
	}

	return w.transformFunc(ctx, w.config.TableName, key, ops, true)
}

// routeAction is called for each route that matches the current row.
// It receives the route config and the resolved document key.
type routeAction func(ctx context.Context, route store.ReplicationRouteConfig, key string) error

// forEachMatchingRoute evaluates route filters, extracts the document key, and
// calls action for every route whose filter matches row. Errors from individual
// routes are collected and joined so one failing route doesn't block the rest.
func (w *ReplicationWorker) forEachMatchingRoute(ctx context.Context, row map[string]any, action routeAction) error {
	var errs []error
	for i, route := range w.config.Routes {
		matched, err := w.routeFilters[i].Evaluate(row)
		if err != nil {
			errs = append(errs, fmt.Errorf("route %s: evaluating filter: %w", route.TargetTable, err))
			continue
		}
		if !matched {
			continue
		}

		keyTpl := route.KeyTemplate
		if keyTpl == "" {
			keyTpl = w.config.KeyTemplate
		}
		key, err := extractKey(row, keyTpl)
		if err != nil {
			errs = append(errs, fmt.Errorf("route %s: extracting key: %w", route.TargetTable, err))
			continue
		}

		if err := action(ctx, route, key); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// processDataChangeRouted evaluates each route's where filter and fans out to matching targets.
func (w *ReplicationWorker) processDataChangeRouted(ctx context.Context, rel *pglogrepl.RelationMessageV2, row map[string]any) error {
	return w.forEachMatchingRoute(ctx, row, func(ctx context.Context, route store.ReplicationRouteConfig, key string) error {
		var resolved []resolvedOp
		var err error
		if route.OnUpdate == nil {
			resolved = autoOnUpdate(rel, row)
		} else {
			resolved, err = resolveTransforms(route.OnUpdate, row)
			if err != nil {
				return fmt.Errorf("route %s: resolving on_update: %w", route.TargetTable, err)
			}
		}

		ops, err := toDBTransformOps(resolved)
		if err != nil {
			return fmt.Errorf("route %s: converting ops: %w", route.TargetTable, err)
		}

		if err := w.transformFunc(ctx, route.TargetTable, key, ops, true); err != nil {
			return fmt.Errorf("route %s: transform: %w", route.TargetTable, err)
		}
		return nil
	})
}

// processDelete handles DELETE events by either deleting the document entirely
// or applying unset transforms to remove this source's fields.
func (w *ReplicationWorker) processDelete(ctx context.Context, rel *pglogrepl.RelationMessageV2, oldTuple *pglogrepl.TupleData) error {
	if oldTuple == nil {
		return fmt.Errorf("DELETE without old tuple data (REPLICA IDENTITY required)")
	}

	row, err := tupleToMap(rel, oldTuple, w.typeMap)
	if err != nil {
		return fmt.Errorf("decoding old tuple: %w", err)
	}

	if len(w.config.Routes) > 0 {
		return w.processDeleteRouted(ctx, rel, row)
	}

	key, err := extractKey(row, w.config.KeyTemplate)
	if err != nil {
		return fmt.Errorf("extracting key: %w", err)
	}

	// Check for $delete_document in OnDelete
	if w.config.OnDelete != nil {
		for _, op := range w.config.OnDelete {
			if op.Op == opDeleteDocument {
				return w.deleteFunc(ctx, w.config.TableName, key)
			}
		}
	}

	var resolved []resolvedOp
	if w.config.OnDelete != nil {
		resolved, err = resolveTransforms(w.config.OnDelete, row)
		if err != nil {
			return fmt.Errorf("resolving on_delete transforms: %w", err)
		}
	} else {
		resolved = autoOnDelete(w.config.OnUpdate, rel)
	}

	ops, err := toDBTransformOps(resolved)
	if err != nil {
		return fmt.Errorf("converting to db transform ops: %w", err)
	}

	return w.transformFunc(ctx, w.config.TableName, key, ops, false)
}

// processDeleteRouted evaluates each route's filter and fans out deletes to matching targets.
func (w *ReplicationWorker) processDeleteRouted(ctx context.Context, rel *pglogrepl.RelationMessageV2, row map[string]any) error {
	return w.forEachMatchingRoute(ctx, row, func(ctx context.Context, route store.ReplicationRouteConfig, key string) error {
		// Check for $delete_document
		for _, op := range route.OnDelete {
			if op.Op == opDeleteDocument {
				if err := w.deleteFunc(ctx, route.TargetTable, key); err != nil {
					return fmt.Errorf("route %s: delete: %w", route.TargetTable, err)
				}
				return nil
			}
		}

		var resolved []resolvedOp
		var err error
		if route.OnDelete != nil {
			resolved, err = resolveTransforms(route.OnDelete, row)
			if err != nil {
				return fmt.Errorf("route %s: resolving on_delete: %w", route.TargetTable, err)
			}
		} else {
			resolved = autoOnDelete(route.OnUpdate, rel)
		}

		ops, err := toDBTransformOps(resolved)
		if err != nil {
			return fmt.Errorf("route %s: converting ops: %w", route.TargetTable, err)
		}

		if err := w.transformFunc(ctx, route.TargetTable, key, ops, false); err != nil {
			return fmt.Errorf("route %s: transform: %w", route.TargetTable, err)
		}
		return nil
	})
}

// tupleToMap decodes a WAL tuple into a map using relation column metadata
// and pgtype.Map for type-aware decoding.
func tupleToMap(rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData, typeMap *pgtype.Map) (map[string]any, error) {
	if tuple == nil {
		return nil, fmt.Errorf("nil tuple data")
	}

	row := make(map[string]any, len(rel.Columns))
	for i, col := range rel.Columns {
		if i >= int(tuple.ColumnNum) {
			break
		}

		tupleCol := tuple.Columns[i]
		switch tupleCol.DataType {
		case pglogrepl.TupleDataTypeNull:
			row[col.Name] = nil

		case pglogrepl.TupleDataTypeToast:
			// Unchanged TOAST value — not included in the row map.
			// NOTE: Route filters that reference TOAST columns may produce false
			// negatives on UPDATE events unless REPLICA IDENTITY FULL is set on
			// the PostgreSQL source table.
			continue

		case pglogrepl.TupleDataTypeText:
			decoded, err := decodeTextColumn(col, tupleCol.Data, typeMap)
			if err != nil {
				// Fall back to string for unknown types
				row[col.Name] = string(tupleCol.Data)
			} else {
				row[col.Name] = decoded
			}

		case pglogrepl.TupleDataTypeBinary:
			decoded, err := decodeBinaryColumn(col, tupleCol.Data, typeMap)
			if err != nil {
				// Fall back to raw bytes as string for unknown types
				row[col.Name] = string(tupleCol.Data)
			} else {
				row[col.Name] = decoded
			}

		default:
			row[col.Name] = string(tupleCol.Data)
		}
	}

	return row, nil
}

// decodeTextColumn decodes a text-format column value using pgtype.
func decodeTextColumn(col *pglogrepl.RelationMessageColumn, data []byte, typeMap *pgtype.Map) (any, error) {
	if _, ok := typeMap.TypeForOID(col.DataType); !ok {
		return string(data), nil
	}

	var value any
	if err := typeMap.Scan(col.DataType, pgtype.TextFormatCode, data, &value); err != nil {
		return nil, fmt.Errorf("scanning text value for column %s (OID %d): %w", col.Name, col.DataType, err)
	}

	return normalizeValue(value), nil
}

// decodeBinaryColumn decodes a binary-format column value using pgtype.
func decodeBinaryColumn(col *pglogrepl.RelationMessageColumn, data []byte, typeMap *pgtype.Map) (any, error) {
	if _, ok := typeMap.TypeForOID(col.DataType); !ok {
		return string(data), nil
	}

	var value any
	if err := typeMap.Scan(col.DataType, pgtype.BinaryFormatCode, data, &value); err != nil {
		return nil, fmt.Errorf("scanning binary value for column %s (OID %d): %w", col.Name, col.DataType, err)
	}

	return normalizeValue(value), nil
}

// normalizeValue converts pgtype-specific Go types to standard types
// that work well with JSON serialization and Antfly transforms.
func normalizeValue(v any) any {
	switch val := v.(type) {
	case pgtype.Numeric:
		f, err := val.Float64Value()
		if err == nil && f.Valid {
			return f.Float64
		}
		// Fall back to string representation
		text, err := val.MarshalJSON()
		if err == nil {
			return string(text)
		}
		return v
	case pgtype.Text:
		if val.Valid {
			return val.String
		}
		return nil
	case pgtype.Bool:
		if val.Valid {
			return val.Bool
		}
		return nil
	case pgtype.Int2:
		if val.Valid {
			return int64(val.Int16)
		}
		return nil
	case pgtype.Int4:
		if val.Valid {
			return int64(val.Int32)
		}
		return nil
	case pgtype.Int8:
		if val.Valid {
			return val.Int64
		}
		return nil
	case pgtype.Float4:
		if val.Valid {
			return float64(val.Float32)
		}
		return nil
	case pgtype.Float8:
		if val.Valid {
			return val.Float64
		}
		return nil
	case pgtype.Timestamp:
		if val.Valid {
			return val.Time
		}
		return nil
	case pgtype.Timestamptz:
		if val.Valid {
			return val.Time
		}
		return nil
	case pgtype.Date:
		if val.Valid {
			return val.Time
		}
		return nil
	case pgtype.UUID:
		if val.Valid {
			return fmt.Sprintf("%x-%x-%x-%x-%x", val.Bytes[0:4], val.Bytes[4:6], val.Bytes[6:8], val.Bytes[8:10], val.Bytes[10:16])
		}
		return nil
	default:
		return v
	}
}

// resolveTransforms evaluates each transform op's Value field against the decoded PG row.
// Column references like {{column}} are replaced with decoded values.
// {{column.key}} navigates into decoded JSONB maps.
// $merge ops expand to individual $set ops.
// Literal values (no {{}}) are used as-is.
func resolveTransforms(ops []store.ReplicationTransformOp, row map[string]any) ([]resolvedOp, error) {
	var result []resolvedOp

	for _, op := range ops {
		switch op.Op {
		case opDeleteDocument:
			result = append(result, resolvedOp{Op: op.Op})
			continue

		case "$merge":
			val, err := resolveValue(op.Value, row)
			if err != nil {
				return nil, fmt.Errorf("resolving $merge value: %w", err)
			}
			m, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("$merge value must resolve to a map, got %T", val)
			}
			for k, v := range m {
				result = append(result, resolvedOp{Op: "$set", Path: k, Value: v})
			}
			continue

		case "$unset":
			result = append(result, resolvedOp{Op: op.Op, Path: op.Path})
			continue

		case "$currentDate":
			result = append(result, resolvedOp{Op: op.Op, Path: op.Path})
			continue
		}

		// For all other ops, resolve the value
		val, err := resolveValue(op.Value, row)
		if err != nil {
			return nil, fmt.Errorf("resolving value for %s %s: %w", op.Op, op.Path, err)
		}
		result = append(result, resolvedOp{Op: op.Op, Path: op.Path, Value: val})
	}

	return result, nil
}

// resolveValue resolves a single value against the row.
// String values containing {{column}} references are substituted.
// Non-string values are passed through as-is.
func resolveValue(value any, row map[string]any) (any, error) {
	strVal, ok := value.(string)
	if !ok {
		// Non-string literal value, use as-is
		return value, nil
	}

	// Check if the entire value is a single column reference like "{{column}}" or "{{column.key}}"
	matches := columnRefPattern.FindAllStringSubmatch(strVal, -1)
	if len(matches) == 0 {
		// No column references, use as literal string
		return strVal, nil
	}

	// If the value is exactly one column reference with no surrounding text,
	// return the typed value directly (preserves int, float, map, etc.)
	if len(matches) == 1 && matches[0][0] == strVal {
		return navigateColumnRef(matches[0][1], row)
	}

	// Multiple references or mixed with text: string interpolation
	result := strVal
	for _, match := range matches {
		colVal, err := navigateColumnRef(match[1], row)
		if err != nil {
			return nil, err
		}
		result = strings.ReplaceAll(result, match[0], fmt.Sprintf("%v", colVal))
	}
	return result, nil
}

// navigateColumnRef resolves a dotted column reference like "column" or "column.key"
// against the decoded row.
func navigateColumnRef(ref string, row map[string]any) (any, error) {
	parts := strings.Split(ref, ".")
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty column reference")
	}

	val, ok := row[parts[0]]
	if !ok {
		return nil, fmt.Errorf("column %q not found in row", parts[0])
	}

	// Navigate into nested maps for dotted references like "column.key"
	for _, part := range parts[1:] {
		m, ok := val.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("cannot navigate into non-map value at %q", part)
		}
		val, ok = m[part]
		if !ok {
			return nil, fmt.Errorf("key %q not found in nested map", part)
		}
	}

	return val, nil
}

// extractKey evaluates the KeyTemplate against the decoded row to produce the
// Antfly document key. If the template contains no {{}} references, it's treated
// as a plain column name.
func extractKey(row map[string]any, keyTemplate string) (string, error) {
	if keyTemplate == "" {
		return "", fmt.Errorf("empty key template")
	}

	// Check if template contains {{}} references
	if !strings.Contains(keyTemplate, "{{") {
		// Plain column name
		val, ok := row[keyTemplate]
		if !ok {
			return "", fmt.Errorf("key column %q not found in row", keyTemplate)
		}
		if val == nil {
			return "", fmt.Errorf("key column %q is null", keyTemplate)
		}
		return fmt.Sprintf("%v", val), nil
	}

	// Template with {{col}} references
	result := keyTemplate
	matches := columnRefPattern.FindAllStringSubmatch(keyTemplate, -1)
	for _, match := range matches {
		val, ok := row[match[1]]
		if !ok {
			return "", fmt.Errorf("key column %q not found in row", match[1])
		}
		if val == nil {
			return "", fmt.Errorf("key column %q is null", match[1])
		}
		result = strings.ReplaceAll(result, match[0], fmt.Sprintf("%v", val))
	}

	return result, nil
}

// toDBTransformOps converts resolved ops to protobuf TransformOp messages.
func toDBTransformOps(ops []resolvedOp) ([]*db.TransformOp, error) {
	result := make([]*db.TransformOp, 0, len(ops))
	for _, op := range ops {
		opType, err := parseOpType(op.Op)
		if err != nil {
			return nil, err
		}

		builder := db.TransformOp_builder{
			Path: normalizeTransformPath(op.Path),
			Op:   opType,
		}

		// Serialize value to JSON bytes for the protobuf
		if op.Value != nil {
			valueBytes, err := json.Marshal(op.Value)
			if err != nil {
				return nil, fmt.Errorf("marshaling value for %s %s: %w", op.Op, op.Path, err)
			}
			builder.Value = valueBytes
		}

		result = append(result, builder.Build())
	}
	return result, nil
}

// normalizeTransformPath ensures the path has a $. prefix as expected by the transform system.
func normalizeTransformPath(path string) string {
	if path == "" {
		return path
	}
	if strings.HasPrefix(path, "$.") {
		return path
	}
	return "$." + path
}

// parseOpType maps a string operation name to the protobuf enum value.
func parseOpType(op string) (db.TransformOp_OpType, error) {
	switch op {
	case "$set":
		return db.TransformOp_SET, nil
	case "$unset":
		return db.TransformOp_UNSET, nil
	case "$inc":
		return db.TransformOp_INC, nil
	case "$push":
		return db.TransformOp_PUSH, nil
	case "$pull":
		return db.TransformOp_PULL, nil
	case "$addToSet":
		return db.TransformOp_ADD_TO_SET, nil
	case "$pop":
		return db.TransformOp_POP, nil
	case "$mul":
		return db.TransformOp_MUL, nil
	case "$min":
		return db.TransformOp_MIN, nil
	case "$max":
		return db.TransformOp_MAX, nil
	case "$currentDate":
		return db.TransformOp_CURRENT_DATE, nil
	case "$rename":
		return db.TransformOp_RENAME, nil
	default:
		return 0, fmt.Errorf("unknown transform operation: %q", op)
	}
}

// autoOnUpdate auto-generates $set for every column in the row (passthrough mode).
func autoOnUpdate(rel *pglogrepl.RelationMessageV2, row map[string]any) []resolvedOp {
	ops := make([]resolvedOp, 0, len(row))
	for _, col := range rel.Columns {
		val, ok := row[col.Name]
		if !ok {
			continue
		}
		ops = append(ops, resolvedOp{
			Op:    "$set",
			Path:  col.Name,
			Value: val,
		})
	}
	return ops
}

// autoOnDelete derives $unset operations from the OnUpdate config.
// If OnUpdate is nil, it unsets every column in the relation.
func autoOnDelete(onUpdate []store.ReplicationTransformOp, rel *pglogrepl.RelationMessageV2) []resolvedOp {
	if onUpdate == nil {
		// No explicit OnUpdate, unset every column in the relation
		ops := make([]resolvedOp, 0, len(rel.Columns))
		for _, col := range rel.Columns {
			ops = append(ops, resolvedOp{Op: "$unset", Path: col.Name})
		}
		return ops
	}

	// Derive $unset from $set paths in OnUpdate
	var ops []resolvedOp
	for _, op := range onUpdate {
		if op.Op == "$set" && op.Path != "" {
			ops = append(ops, resolvedOp{Op: "$unset", Path: op.Path})
		}
	}
	return ops
}

// SlotName derives a replication slot name from table and PG table names.
// Max 63 chars, alphanumeric + underscore only.
func SlotName(tableName, pgTable string) string {
	return sanitizePGIdentifier("antfly_"+tableName+"_"+pgTable, 63)
}

// PublicationName derives a publication name from table and PG table names.
// Max 63 chars, alphanumeric + underscore only.
func PublicationName(tableName, pgTable string) string {
	return sanitizePGIdentifier("antfly_pub_"+tableName+"_"+pgTable, 63)
}

// sanitizePGIdentifier replaces non-alphanumeric characters with underscores
// and truncates to maxLen.
func sanitizePGIdentifier(s string, maxLen int) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune('_')
		}
	}
	result := b.String()
	if len(result) > maxLen {
		result = result[:maxLen]
	}
	return result
}

// pgQuoteIdentifier quotes a PostgreSQL identifier to prevent SQL injection.
func pgQuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// pgQuoteLiteral quotes a string as a SQL literal (single quotes, escaping embedded quotes).
func pgQuoteLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

// isPermanentReplicationError classifies errors as permanent (stop retrying)
// or transient (retry with backoff).
func isPermanentReplicationError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "28P01", "28000": // invalid_password, invalid_authorization_specification
			return true
		case "42P01": // undefined_table
			return true
		case "3D000": // invalid_catalog_name (database does not exist)
			return true
		case "42501": // insufficient_privilege
			return true
		}
	}
	return false
}
