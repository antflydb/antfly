module github.com/antflydb/antfly/pkg/termite

go 1.26.0

// Pin deps compatible with oapi-codegen v2.5.1.
// kin-openapi v0.134.0 breaks oapi-codegen (MappingRef type change) and
// oasdiff/yaml v0.0.1 breaks kin-openapi v0.133.0 (OriginOpt API change).
replace (
	github.com/antflydb/antfly/pkg/libaf => ../libaf
	github.com/getkin/kin-openapi v0.134.0 => github.com/getkin/kin-openapi v0.133.0
	github.com/gomlx/gomlx => github.com/ajroetker/gomlx v0.0.0-antfly011
	github.com/gomlx/onnx-gomlx => github.com/ajroetker/onnx-gomlx v0.0.0-antfly011
	github.com/knights-analytics/ortgenai => github.com/ajroetker/ortgenai v0.1.1-antfly002
	github.com/kovidgoyal/imaging => github.com/antflydb/imaging v1.8.21-antfly001
	github.com/oasdiff/yaml v0.0.1 => github.com/oasdiff/yaml v0.0.0-20250309154309-f31be36b4037
	github.com/oasdiff/yaml3 v0.0.1 => github.com/oasdiff/yaml3 v0.0.0-20250309153720-d2182401db90
)

require (
	github.com/ajroetker/go-highway v0.0.13-0.20260309234436-8d249c4caa48
	github.com/antflydb/antfly/pkg/libaf v0.0.1
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/daulet/tokenizers v1.26.0
	github.com/eliben/go-sentencepiece v0.7.0
	github.com/getkin/kin-openapi v0.134.0
	github.com/goccy/go-json v0.10.6
	github.com/gomlx/go-coreml/gomlx v0.0.0-20260301010621-8fdf6ad8655e
	github.com/gomlx/go-huggingface v0.3.4
	github.com/gomlx/gomlx v0.27.2
	github.com/gomlx/onnx-gomlx v0.0.0-00010101000000-000000000000
	github.com/hajimehoshi/go-mp3 v0.3.4
	github.com/jellydator/ttlcache/v3 v3.4.0
	github.com/knights-analytics/ortgenai v0.1.0
	github.com/kovidgoyal/imaging v1.8.20
	github.com/nikolalohinski/gonja/v2 v2.7.0
	github.com/oapi-codegen/runtime v1.3.0
	github.com/prometheus/client_golang v1.23.2
	github.com/spf13/cobra v1.10.2
	github.com/spf13/pflag v1.0.10
	github.com/spf13/viper v1.21.0
	github.com/stretchr/testify v1.11.1
	github.com/yalue/onnxruntime_go v1.27.0
	go.uber.org/zap v1.27.1
	golang.org/x/image v0.37.0
	golang.org/x/sync v0.20.0
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dprotaso/go-yit v0.0.0-20250513224043-18a80f8f6df4 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-openapi/jsonpointer v0.22.5 // indirect
	github.com/go-openapi/swag/jsonname v0.25.5 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/gomlx/exceptions v0.0.3 // indirect
	github.com/gomlx/go-coreml v0.0.0-20260301010621-8fdf6ad8655e // indirect
	github.com/gomlx/go-xla v0.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/kovidgoyal/go-parallel v1.1.1 // indirect
	github.com/mailru/easyjson v0.9.2 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.99 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oapi-codegen/oapi-codegen/v2 v2.5.1 // indirect
	github.com/oasdiff/yaml v0.0.1 // indirect
	github.com/oasdiff/yaml3 v0.0.1 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/sagikazarmark/locafero v0.12.0 // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/speakeasy-api/jsonpath v0.6.2 // indirect
	github.com/speakeasy-api/openapi-overlay v0.10.3 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/tinylib/msgp v1.6.3 // indirect
	github.com/vmware-labs/yaml-jsonpath v0.3.2 // indirect
	github.com/woodsbury/decimal128 v1.4.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/crypto v0.49.0 // indirect
	golang.org/x/exp v0.0.0-20260312153236-7ab1446f8b90 // indirect
	golang.org/x/mod v0.34.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/term v0.41.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	golang.org/x/tools v0.43.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
)

tool github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen
