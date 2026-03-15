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

package db

//go:generate protoc --go_out=. --go_opt=paths=source_relative ops.proto
import (
	"context"
	"errors"
	"fmt"
	"path"
	"sync"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/google/uuid"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"google.golang.org/protobuf/proto"
)

// Shared zstd encoder/decoder instances.
// EncodeAll and DecodeAll are concurrent-safe on a shared instance.
var (
	zstdEncoder, _ = zstd.NewWriter(nil)
	zstdDecoder, _ = zstd.NewReader(nil)

	// marshalPool provides reusable buffers for the intermediate
	// proto.Marshal output in EncodeProto. The compressed result
	// returned by EncodeAll is a fresh allocation owned by the caller.
	marshalPool = sync.Pool{
		New: func() any {
			b := make([]byte, 0, 4096)
			return &b
		},
	}
)

// NewDeleteIndexOp creates a new Op for deleting an index.
func NewDeleteIndexOp(name string) *Op {
	kvOp := &Op{}
	kvOp.SetOp(Op_OpDeleteIndex)
	kvOp.SetDeleteIndex(DeleteIndexOp_builder{Name: name}.Build())
	return kvOp
}

// NewAddIndexOp creates a new Op for adding an index.
func NewAddIndexOp(config *indexes.IndexConfig) (*Op, error) {
	if config == nil {
		return nil, errors.New("index config cannot be nil")
	}
	pbConfig, err := config.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("marshalling index config: %w", err)
	}

	kvOp := &Op{}
	kvOp.SetOp(Op_OpAddIndex)
	kvOp.SetAddIndex(AddIndexOp_builder{
		Name:   config.Name,
		Type:   string(config.Type),
		Config: pbConfig,
	}.Build())
	return kvOp, nil
}

// NewSplitOp creates a new Op for splitting a shard.
func NewSplitOp(newShardID uint64, splitKey []byte) *Op {
	kvOp := &Op{}
	kvOp.SetOp(Op_OpSplit)
	kvOp.SetSplit(SplitOp_builder{
		NewShardId: newShardID,
		SplitKey:   splitKey,
	}.Build())
	return kvOp
}

// NewSetRangeOp creates a new Op for setting the byte range.
func NewSetRangeOp(byteRange [2][]byte) *Op {
	kvOp := Op{}
	kvOp.SetOp(Op_OpSetRange)
	kvOp.SetSetRange(SetRangeOp_builder{
		StartKey: byteRange[0],
		EndKey:   byteRange[1],
	}.Build())
	return &kvOp
}

// NewUpdateSchemaOp creates a new Op for updating the schema.
func NewUpdateSchemaOp(schema *schema.TableSchema) (*Op, error) {
	if schema == nil {
		return nil, errors.New("schema cannot be nil")
	}
	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("marshalling schema: %w", err)
	}
	kvOp := &Op_builder{
		Op: Op_OpUpdateSchema,
		UpdateSchema: UpdateSchemaOp_builder{
			Schema: schemaBytes,
		}.Build(),
	}
	return kvOp.Build(), nil
}

func init() {
	uuid.EnableRandPool()
}

// NewBatchOp creates a new Op for a batch operation.
func NewBatchOp(batch *BatchOp) *Op {
	kvOp := &Op_builder{
		Op:    Op_OpBatch,
		Batch: batch,
	}
	return kvOp.Build()
}

// WritesFromTuples converts [][2][]byte to []*Write.
// This helper simplifies conversion from the simple tuple format to protobuf Write structs.
func WritesFromTuples(tuples [][2][]byte) []*Write {
	if len(tuples) == 0 {
		return nil
	}
	writes := make([]*Write, len(tuples))
	for i, t := range tuples {
		writes[i] = Write_builder{
			Key:   t[0],
			Value: t[1],
		}.Build()
	}
	return writes
}

// WritesToTuples converts []*Write to [][2][]byte.
// This helper simplifies conversion from protobuf Write structs to the simple tuple format.
func WritesToTuples(writes []*Write) [][2][]byte {
	if len(writes) == 0 {
		return nil
	}
	tuples := make([][2][]byte, len(writes))
	for i, w := range writes {
		tuples[i] = [2][]byte{w.GetKey(), w.GetValue()}
	}
	return tuples
}

// EncodeProto encodes an Op with zstd compression and returns the
// compressed bytes. The returned slice is freshly allocated and safe
// to pass to Raft without copying.
// Uses a shared zstd encoder via EncodeAll (concurrent-safe).
func EncodeProto(kv *Op) ([]byte, error) {
	bufp := marshalPool.Get().(*[]byte)
	pbData, err := proto.MarshalOptions{}.MarshalAppend((*bufp)[:0], kv)
	if err != nil {
		marshalPool.Put(bufp)
		return nil, fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	compressed := zstdEncoder.EncodeAll(pbData, nil)

	// Return the marshal buffer to the pool. The compressed slice is a
	// separate allocation owned by the caller.
	*bufp = pbData
	marshalPool.Put(bufp)

	return compressed, nil
}

// DecodeProto decodes zstd-compressed data into an Op.
// Uses a shared zstd decoder via DecodeAll (concurrent-safe).
func DecodeProto(data []byte, kvStoreOp *Op) error {
	if len(data) == 0 {
		return errors.New("cannot decode empty data")
	}

	decompressed, err := zstdDecoder.DecodeAll(data, nil)
	if err != nil {
		return fmt.Errorf("failed to decompress data: %w", err)
	}

	if len(decompressed) == 0 {
		return errors.New("decompressed data is empty")
	}

	if err := proto.Unmarshal(decompressed, kvStoreOp); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return nil
}

// WriteBackupToBlobStore writes a backup file to an S3-compatible blob store.
func WriteBackupToBlobStore(ctx context.Context, bucketURL, filePath string, s3Info *common.S3Info) error {
	// e.g. "s3://my-bucket-name/optional/prefix"
	bucket, prefix, err := common.ParseS3URL(bucketURL)
	if err != nil {
		return fmt.Errorf("parsing bucket URL: %w", err)
	}
	minioClient, err := s3Info.NewMinioClient()
	if err != nil {
		return fmt.Errorf("creating S3 client: %w", err)
	}
	if ok, err := minioClient.BucketExists(ctx, bucket); err != nil {
		return fmt.Errorf("checking if bucket %s exists: %w", bucket, err)
	} else if !ok {
		return fmt.Errorf("bucket %s does not exist", bucket)
	}
	// Construct object key with optional prefix
	objectKey := path.Base(filePath)
	if prefix != "" {
		objectKey = path.Join(prefix, objectKey)
	}
	if _, err := minioClient.FPutObject(ctx, bucket, objectKey, filePath, minio.PutObjectOptions{}); err != nil {
		_ = minioClient.RemoveObject(
			context.Background(),
			bucket,
			objectKey,
			minio.RemoveObjectOptions{
				ForceDelete: true,
			},
		)
		return fmt.Errorf("uploading file to object store: %w", err)
	}
	return nil
}
