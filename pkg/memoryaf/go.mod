module github.com/antflydb/antfly/pkg/memoryaf

go 1.26.0

replace github.com/antflydb/antfly/pkg/libaf => ../libaf

replace github.com/antflydb/antfly/pkg/termite-client => ../termite-client

require (
	github.com/antflydb/antfly v0.1.0
	github.com/antflydb/antfly/pkg/client v0.0.0
	github.com/antflydb/antfly/pkg/termite-client v0.0.0
	github.com/google/uuid v1.6.0
	github.com/modelcontextprotocol/go-sdk v1.4.1
	go.uber.org/zap v1.27.1
)

require (
	github.com/antflydb/antfly/pkg/libaf v0.0.1 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/getkin/kin-openapi v0.134.0 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-json-experiment/json v0.0.0-20260214004413-d219187c3433 // indirect
	github.com/go-openapi/jsonpointer v0.22.5 // indirect
	github.com/go-openapi/swag/jsonname v0.25.5 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/jsonschema-go v0.4.2 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/kaptinlin/go-i18n v0.2.12 // indirect
	github.com/kaptinlin/jsonpointer v0.4.17 // indirect
	github.com/kaptinlin/jsonschema v0.7.6 // indirect
	github.com/kaptinlin/messageformat-go v0.4.18 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/mailru/easyjson v0.9.2 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.99 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/oapi-codegen/runtime v1.3.0 // indirect
	github.com/oasdiff/yaml v0.0.1 // indirect
	github.com/oasdiff/yaml3 v0.0.1 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/tinylib/msgp v1.6.3 // indirect
	github.com/woodsbury/decimal128 v1.4.0 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
)

replace github.com/antflydb/antfly/pkg/client => ../client
