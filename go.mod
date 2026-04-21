module github.com/antflydb/antfly

go 1.26.0

replace (
	github.com/antflydb/antfly/pkg/client => ./pkg/client
	github.com/antflydb/antfly/pkg/evalaf => ./pkg/evalaf
	github.com/antflydb/antfly/pkg/evalaf/plugins/antfly => ./pkg/evalaf/plugins/antfly
	github.com/antflydb/antfly/pkg/generating => ./pkg/generating
	github.com/antflydb/antfly/pkg/genkit/openrouter => ./pkg/genkit/openrouter
	github.com/antflydb/antfly/pkg/libaf => ./pkg/libaf
	github.com/antflydb/antfly/pkg/termite => ./pkg/termite
	github.com/antflydb/antfly/pkg/termite-client => ./pkg/termite-client
)

replace github.com/danaugrs/go-tsne => github.com/danaugrs/go-tsne/tsne v0.0.0-20220306155740-2250969e057f

replace (
	github.com/blevesearch/bleve/v2 => github.com/antflydb/bleve/v2 v2.5.8-antfly002
	github.com/blevesearch/zapx/v17 => github.com/antflydb/zapx/v17 v17.0.2-antfly004
)

replace github.com/tidwall/wal => github.com/ajroetker/wal v0.0.0-antfly000

// Pin deps compatible with oapi-codegen v2.5.1.
// kin-openapi v0.134.0 breaks oapi-codegen (MappingRef type change) and
// panics in InternalizeRefs on schemas with empty refs.
// oasdiff/yaml v0.0.1 breaks kin-openapi v0.133.0 (OriginOpt API change).
// go-yit 20250909 pulls in go.yaml.in/yaml/v4 which breaks yaml-jsonpath.
replace (
	github.com/dprotaso/go-yit v0.0.0-20250909171706-0a81c39169bc => github.com/dprotaso/go-yit v0.0.0-20250513224043-18a80f8f6df4
	github.com/getkin/kin-openapi v0.134.0 => github.com/getkin/kin-openapi v0.133.0
	github.com/oasdiff/yaml v0.0.1 => github.com/oasdiff/yaml v0.0.0-20250309154309-f31be36b4037
	github.com/oasdiff/yaml3 v0.0.1 => github.com/oasdiff/yaml3 v0.0.0-20250309153720-d2182401db90
)

replace (
	github.com/gomlx/gomlx => github.com/ajroetker/gomlx v0.0.0-antfly011
	github.com/gomlx/onnx-gomlx => github.com/ajroetker/onnx-gomlx v0.0.0-antfly011
	github.com/knights-analytics/ortgenai => github.com/ajroetker/ortgenai v0.1.1-antfly002
	github.com/kovidgoyal/imaging => github.com/antflydb/imaging v1.8.21-antfly001
)

require (
	cloud.google.com/go/speech v1.30.0
	codeberg.org/readeck/go-readability/v2 v2.1.1
	github.com/a2aproject/a2a-go v0.3.12
	github.com/ajroetker/go-highway v0.0.13-0.20260309234436-8d249c4caa48
	github.com/ajroetker/pdf v0.0.1-antfly001
	github.com/ajroetker/pdf/render v0.0.1-antfly003
	github.com/alpkeskin/gotoon v0.1.1
	github.com/antflydb/antfly/pkg/client v0.0.1
	github.com/antflydb/antfly/pkg/evalaf v0.0.0
	github.com/antflydb/antfly/pkg/generating v0.0.0
	github.com/antflydb/antfly/pkg/libaf v0.0.1
	github.com/antflydb/antfly/pkg/termite v0.0.0
	github.com/antflydb/antfly/pkg/termite-client v0.0.0
	github.com/aws/aws-sdk-go-v2/config v1.32.12
	github.com/aws/aws-sdk-go-v2/service/bedrockruntime v1.50.2
	github.com/blevesearch/bleve/v2 v2.5.7
	github.com/blevesearch/bleve_index_api v1.3.4
	github.com/blevesearch/sear v0.4.1
	github.com/casbin/casbin/v3 v3.10.0
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/chewxy/math32 v1.11.1
	github.com/cockroachdb/pebble/v2 v2.1.4
	github.com/cohere-ai/cohere-go/v2 v2.16.2
	github.com/danaugrs/go-tsne v0.0.0-20200708172100-6b7d1d577fd3
	github.com/firebase/genkit/go v1.5.0
	github.com/getkin/kin-openapi v0.134.0
	github.com/goccy/go-json v0.10.6
	github.com/golang/snappy v1.0.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pglogrepl v0.0.0-20251213150135-2e8d0df862c1
	github.com/jackc/pgx/v5 v5.8.0
	github.com/jellydator/ttlcache/v3 v3.4.0
	github.com/johannesboyne/gofakes3 v0.0.0-20250916175020-ebf3e50324d3
	github.com/kaptinlin/jsonschema v0.7.6
	github.com/klauspost/compress v1.18.5
	github.com/minio/minio-go/v7 v7.0.99
	github.com/modelcontextprotocol/go-sdk v1.4.1
	github.com/oapi-codegen/runtime v1.3.0
	github.com/openai/openai-go v1.12.0
	github.com/puzpuzpuz/xsync/v4 v4.4.0
	github.com/quic-go/quic-go v0.59.0
	github.com/revrost/go-openrouter v1.1.7
	github.com/samber/go-singleflightx v0.3.2
	github.com/sethvargo/go-retry v0.3.0
	github.com/stretchr/testify v1.11.1
	github.com/theory/jsonpath v0.11.0
	go.etcd.io/raft/v3 v3.6.0
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.27.1
	golang.org/x/exp v0.0.0-20260312153236-7ab1446f8b90
	golang.org/x/image v0.37.0
	golang.org/x/sync v0.20.0
	gonum.org/v1/gonum v0.17.0
	gonum.org/v1/plot v0.16.0
	google.golang.org/genai v1.51.0
	mvdan.cc/xurls/v2 v2.6.0
)

require (
	charm.land/lipgloss/v2 v2.0.0-beta.3.0.20251106193318-19329a3e8410 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys v1.4.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/internal v1.2.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/ajroetker/go-jpeg2000 v0.0.2 // indirect
	github.com/antflydb/antfly/pkg/genkit/openrouter v0.0.0 // indirect
	github.com/avast/retry-go/v4 v4.7.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.8 // indirect
	github.com/axiomhq/hyperloglog v0.2.6 // indirect
	github.com/blevesearch/zapx/v17 v17.0.3-0.20260311100439-7e7434b4f844 // indirect
	github.com/charmbracelet/ultraviolet v0.0.0-20251112162901-46098fed0961 // indirect
	github.com/charmbracelet/x/termios v0.1.1 // indirect
	github.com/charmbracelet/x/windows v0.2.2 // indirect
	github.com/clipperhouse/displaywidth v0.11.0 // indirect
	github.com/coreos/go-oidc/v3 v3.17.0 // indirect
	github.com/creack/pty v1.1.24 // indirect
	github.com/daulet/tokenizers v1.26.0 // indirect
	github.com/dgryski/go-metro v0.0.0-20250106013310-edb8663e5e33 // indirect
	github.com/earthboundkid/versioninfo/v2 v2.24.1 // indirect
	github.com/eliben/go-sentencepiece v0.7.0 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/gofrs/flock v0.13.0 // indirect
	github.com/gomlx/exceptions v0.0.3 // indirect
	github.com/gomlx/go-coreml v0.0.0-20260301010621-8fdf6ad8655e // indirect
	github.com/gomlx/go-coreml/gomlx v0.0.0-20260301010621-8fdf6ad8655e // indirect
	github.com/gomlx/go-huggingface v0.3.4 // indirect
	github.com/gomlx/go-xla v0.2.2 // indirect
	github.com/gomlx/gomlx v0.27.2 // indirect
	github.com/gomlx/onnx-gomlx v0.3.5-0.20260130173634-2497f2c7652f // indirect
	github.com/google/go-github/v78 v78.0.0 // indirect
	github.com/hajimehoshi/go-mp3 v0.3.4 // indirect
	github.com/hhrutter/lzw v1.0.0 // indirect
	github.com/hhrutter/pkcs7 v0.2.0 // indirect
	github.com/hhrutter/tiff v1.0.2 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/kamstrup/intmap v0.5.2 // indirect
	github.com/kaptinlin/jsonpointer v0.4.17 // indirect
	github.com/knights-analytics/ortgenai v0.1.0 // indirect
	github.com/kovidgoyal/go-parallel v1.1.1 // indirect
	github.com/kovidgoyal/imaging v1.8.20 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/modelcontextprotocol/registry v1.3.10 // indirect
	github.com/nikolalohinski/gonja/v2 v2.7.0 // indirect
	github.com/pdfcpu/pdfcpu v0.11.1 // indirect
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/sigstore/rekor-tiles/v2 v2.0.1 // indirect
	github.com/sigstore/timestamp-authority/v2 v2.0.3 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yalue/onnxruntime_go v1.27.0 // indirect
	github.com/ysmood/fetchup v0.2.4 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.38.0 // indirect
	go.shabbyrobe.org/gocovmerge v0.0.0-20230507111327-fa4f82cfbf4d // indirect
	gotest.tools/v3 v3.5.2 // indirect
)

require (
	github.com/Masterminds/semver v1.5.0
	github.com/google/dotprompt/go v0.0.0-20260227225921-0911cf9ecf0e
	github.com/mbleigh/raymond v0.0.0-20250414171441-6b3a58ab9e0a
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
	golang.org/x/telemetry v0.0.0-20260311193753-579e4da9a98c // indirect
)

require (
	codeberg.org/go-fonts/liberation v0.5.0 // indirect
	codeberg.org/go-latex/latex v0.2.0 // indirect
	codeberg.org/go-pdf/fpdf v0.11.1 // indirect
	git.sr.ht/~sbinet/gg v0.7.0 // indirect
	github.com/PuerkitoBio/goquery v1.12.0
	github.com/RaduBerinde/axisds v0.1.0 // indirect
	github.com/RaduBerinde/btreemap v0.0.0-20260105202824-d3184786f603 // indirect
	github.com/ajstarks/svgo v0.0.0-20211024235047-1546f124cd8b // indirect
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/araddon/dateparse v0.0.0-20210429162001-6b43995a97de // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/charmbracelet/x/exp/color v0.0.0-20251006100439-2151805163c8 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/go-json-experiment/json v0.0.0-20260214004413-d219187c3433 // indirect
	github.com/go-openapi/swag/cmdutils v0.25.4 // indirect
	github.com/go-openapi/swag/conv v0.25.5 // indirect
	github.com/go-openapi/swag/fileutils v0.25.4 // indirect
	github.com/go-openapi/swag/jsonname v0.25.5 // indirect
	github.com/go-openapi/swag/jsonutils v0.25.5 // indirect
	github.com/go-openapi/swag/loading v0.25.5 // indirect
	github.com/go-openapi/swag/mangling v0.25.4 // indirect
	github.com/go-openapi/swag/netutils v0.25.4 // indirect
	github.com/go-openapi/swag/stringutils v0.25.4 // indirect
	github.com/go-openapi/swag/typeutils v0.25.5 // indirect
	github.com/go-openapi/swag/yamlutils v0.25.5 // indirect
	github.com/go-shiori/dom v0.0.0-20230515143342-73569d674e1c // indirect
	github.com/gogs/chardet v0.0.0-20211120154057-b7413eaefb8f // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/jsonschema-go v0.4.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/ipfs/boxo v0.35.2 // indirect
	github.com/kaptinlin/messageformat-go v0.4.18 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/minio/minlz v1.1.0 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/panjf2000/ants/v2 v2.12.0
	github.com/tidwall/sjson v1.2.5 // indirect
	github.com/transparency-dev/formats v0.0.0-20251110090430-df1ffe27d819 // indirect
	github.com/woodsbury/decimal128 v1.4.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
)

require (
	al.essio.dev/pkg/shellescape v1.6.0 // indirect
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go/kms v1.26.0 // indirect
	cloud.google.com/go/monitoring v1.24.3 // indirect
	cloud.google.com/go/storage v1.58.0 // indirect
	code.gitea.io/sdk/gitea v0.22.1 // indirect
	dario.cat/mergo v1.0.2 // indirect
	github.com/42wim/httpsig v1.2.3 // indirect
	github.com/AlekSi/pointer v1.2.0 // indirect
	github.com/Azure/azure-sdk-for-go v68.0.0+incompatible // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.20.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys v0.10.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/internal v0.7.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.3 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.30 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.24 // indirect
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.13 // indirect
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.7 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.1 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.2 // indirect
	github.com/Azure/go-autorest/tracing v0.6.1 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.6.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.30.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.54.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.54.0 // indirect
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/sprig/v3 v3.3.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/ProtonMail/go-crypto v1.3.0 // indirect
	github.com/agnivade/levenshtein v1.2.1 // indirect
	github.com/anchore/bubbly v0.0.0-20250717181826-8a411f9d8cbf // indirect
	github.com/anchore/go-logger v0.0.0-20251106021608-a5b0513fa9a9 // indirect
	github.com/anchore/go-macholibre v0.0.0-20250826193721-3cd206ca93aa // indirect
	github.com/anchore/quill v0.5.1 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20230301143203-a9d515a09cc2 // indirect
	github.com/atc0005/go-teams-notify/v2 v2.14.0 // indirect
	github.com/aws/aws-sdk-go v1.55.8 // indirect
	github.com/aws/aws-sdk-go-v2 v1.41.4
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.7 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.12 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.20.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/ecr v1.51.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ecrpublic v1.38.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/kms v1.49.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.93.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.9 // indirect
	github.com/aws/smithy-go v1.24.2 // indirect
	github.com/awslabs/amazon-ecr-credential-helper/ecr-login v0.11.0 // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/blacktop/go-dwarf v1.0.14 // indirect
	github.com/blacktop/go-macho v1.1.257 // indirect
	github.com/blakesmith/ar v0.0.0-20190502131153-809d4375e1fb // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/bluesky-social/indigo v0.0.0-20251112071842-bc4a2419e924 // indirect
	github.com/bmatcuk/doublestar/v4 v4.10.0 // indirect
	github.com/buger/jsonparser v1.1.2 // indirect
	github.com/caarlos0/env/v11 v11.3.1 // indirect
	github.com/caarlos0/go-reddit/v3 v3.0.1 // indirect
	github.com/caarlos0/go-shellwords v1.0.12 // indirect
	github.com/caarlos0/go-version v0.2.2 // indirect
	github.com/caarlos0/log v0.5.2 // indirect
	github.com/casbin/govaluate v1.10.0 // indirect
	github.com/cavaliergopher/cpio v1.0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/charmbracelet/bubbletea v1.3.10 // indirect
	github.com/charmbracelet/colorprofile v0.4.3 // indirect
	github.com/charmbracelet/fang v0.4.4 // indirect
	github.com/charmbracelet/lipgloss v1.1.0 // indirect
	github.com/charmbracelet/lipgloss/v2 v2.0.0-beta1 // indirect
	github.com/charmbracelet/x/ansi v0.11.6 // indirect
	github.com/charmbracelet/x/cellbuf v0.0.15 // indirect
	github.com/charmbracelet/x/exp/charmtone v0.0.0-20251112221808-faa4aaac98c3 // indirect
	github.com/charmbracelet/x/term v0.2.2 // indirect
	github.com/chrismellard/docker-credential-acr-env v0.0.0-20230304212654-82a0ddb27589 // indirect
	github.com/cloudflare/circl v1.6.3 // indirect
	github.com/cncf/xds/go v0.0.0-20251210132809-ee656c7534f5 // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/stargz-snapshotter/estargz v0.18.1 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/cyberphone/json-canonicalization v0.0.0-20241213102144-19d51d7fe467 // indirect
	github.com/cyphar/filepath-securejoin v0.6.0 // indirect
	github.com/davidmz/go-pageant v1.0.2 // indirect
	github.com/dghubble/go-twitter v0.0.0-20221104224141-912508c3888b // indirect
	github.com/dghubble/oauth1 v0.7.3 // indirect
	github.com/dghubble/sling v1.4.2 // indirect
	github.com/digitorus/pkcs7 v0.0.0-20250730155240-ffadbf3f398c // indirect
	github.com/digitorus/timestamp v0.0.0-20250524132541-c45532741eea // indirect
	github.com/dimchansky/utfbom v1.1.1 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/docker/cli v29.0.3+incompatible // indirect
	github.com/docker/distribution v2.8.3+incompatible // indirect
	github.com/docker/docker v28.5.2+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.9.4 // indirect
	github.com/docker/go-connections v0.6.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dprotaso/go-yit v0.0.0-20250909171706-0a81c39169bc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.36.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.0 // indirect
	github.com/erikgeiser/coninput v0.0.0-20211004153227-1c3628e74d0f // indirect
	github.com/evanphx/json-patch/v5 v5.9.11 // indirect
	github.com/gabriel-vasile/mimetype v1.4.11 // indirect
	github.com/github/smimesign v0.2.0 // indirect
	github.com/go-fed/httpsig v1.1.0 // indirect
	github.com/go-git/gcfg v1.5.1-0.20230307220236-3a3c6141e376 // indirect
	github.com/go-git/go-billy/v5 v5.6.2 // indirect
	github.com/go-git/go-git/v5 v5.16.3 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.3 // indirect
	github.com/go-openapi/analysis v0.24.1 // indirect
	github.com/go-openapi/errors v0.22.4 // indirect
	github.com/go-openapi/jsonpointer v0.22.5 // indirect
	github.com/go-openapi/jsonreference v0.21.4 // indirect
	github.com/go-openapi/loads v0.23.2 // indirect
	github.com/go-openapi/runtime v0.29.2 // indirect
	github.com/go-openapi/spec v0.22.1 // indirect
	github.com/go-openapi/strfmt v0.25.0 // indirect
	github.com/go-openapi/swag v0.25.4 // indirect
	github.com/go-openapi/validate v0.25.1 // indirect
	github.com/go-restruct/restruct v1.2.0-alpha // indirect
	github.com/go-telegram-bot-api/telegram-bot-api/v5 v5.5.1 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/google/certificate-transparency-go v1.3.2 // indirect
	github.com/google/go-containerregistry v0.20.7 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/ko v0.18.0 // indirect
	github.com/google/rpmpack v0.7.1 // indirect
	github.com/google/wire v0.7.0 // indirect
	github.com/goreleaser/chglog v0.7.3 // indirect
	github.com/goreleaser/fileglob v1.4.0 // indirect
	github.com/goreleaser/goreleaser/v2 v2.13.1 // indirect
	github.com/goreleaser/nfpm/v2 v2.44.0 // indirect
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.8 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/huandu/xstrings v1.5.0 // indirect
	github.com/in-toto/attestation v1.1.2 // indirect
	github.com/in-toto/in-toto-golang v0.9.0 // indirect
	github.com/invopop/jsonschema v0.13.0 // indirect
	github.com/ipfs/bbloom v0.0.4 // indirect
	github.com/ipfs/go-block-format v0.2.3 // indirect
	github.com/ipfs/go-cid v0.6.0 // indirect
	github.com/ipfs/go-datastore v0.9.0 // indirect
	github.com/ipfs/go-ipfs-blockstore v1.3.1 // indirect
	github.com/ipfs/go-ipfs-ds-help v1.1.1 // indirect
	github.com/ipfs/go-ipld-cbor v0.2.1 // indirect
	github.com/ipfs/go-ipld-format v0.6.3 // indirect
	github.com/ipfs/go-log v1.0.5 // indirect
	github.com/ipfs/go-log/v2 v2.9.0 // indirect
	github.com/ipfs/go-metrics-interface v0.3.0 // indirect
	github.com/jbenet/go-context v0.0.0-20150711004518-d14ea06fba99 // indirect
	github.com/jedisct1/go-minisign v0.0.0-20241212093149-d2f9f49435c7 // indirect
	github.com/jmespath/go-jmespath v0.4.1-0.20220621161143-b0104c826a24 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/kaptinlin/go-i18n v0.2.12 // indirect
	github.com/kevinburke/ssh_config v1.4.0 // indirect
	github.com/klauspost/pgzip v1.2.6 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.3.0 // indirect
	github.com/mailru/easyjson v0.9.2 // indirect
	github.com/mattn/go-localereader v0.0.2-0.20220822084749-2491eb6c1c75 // indirect
	github.com/mattn/go-mastodon v0.0.10 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/muesli/ansi v0.0.0-20230316100256-276c6243b2f6 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/muesli/mango v0.2.0 // indirect
	github.com/muesli/mango-cobra v1.3.0 // indirect
	github.com/muesli/mango-pflag v0.2.0 // indirect
	github.com/muesli/roff v0.1.0 // indirect
	github.com/muesli/termenv v0.16.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multihash v0.2.3 // indirect
	github.com/multiformats/go-varint v0.1.0 // indirect
	github.com/oapi-codegen/oapi-codegen/v2 v2.5.1 // indirect
	github.com/oasdiff/yaml v0.0.1 // indirect
	github.com/oasdiff/yaml3 v0.0.1 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/ollama/ollama v0.18.2
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pjbgf/sha1cd v0.5.0 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/polydawn/refmt v0.89.1-0.20221221234430-40501e09de1f // indirect
	github.com/rs/xid v1.6.0
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/sagikazarmark/locafero v0.12.0 // indirect
	github.com/sassoftware/relic v7.2.1+incompatible // indirect
	github.com/scylladb/go-set v1.0.3-0.20200225121959-cc7b2070d91e // indirect
	github.com/secure-systems-lab/go-securesystemslib v0.10.0 // indirect
	github.com/sergi/go-diff v1.4.0 // indirect
	github.com/shibumi/go-pathspec v1.3.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/sigstore/cosign/v2 v2.6.2 // indirect
	github.com/sigstore/protobuf-specs v0.5.0 // indirect
	github.com/sigstore/rekor v1.4.3 // indirect
	github.com/sigstore/sigstore v1.10.4 // indirect
	github.com/sigstore/sigstore-go v1.1.4 // indirect
	github.com/skeema/knownhosts v1.3.2 // indirect
	github.com/slack-go/slack v0.17.3 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/speakeasy-api/jsonpath v0.6.2 // indirect
	github.com/speakeasy-api/openapi-overlay v0.10.3 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/theupdateframework/go-tuf v0.7.0 // indirect
	github.com/theupdateframework/go-tuf/v2 v2.4.1 // indirect
	github.com/tinylib/msgp v1.6.3 // indirect
	github.com/tomnomnom/linkheader v0.0.0-20250811210735-e5fe3b51442e // indirect
	github.com/transparency-dev/merkle v0.0.2 // indirect
	github.com/ulikunitz/xz v0.5.15 // indirect
	github.com/vbatts/tar-split v0.12.2 // indirect
	github.com/vmware-labs/yaml-jsonpath v0.3.2 // indirect
	github.com/wagoodman/go-partybus v0.0.0-20230516145632-8ccac152c651 // indirect
	github.com/wagoodman/go-progress v0.0.0-20230925121702-07e42b3cdba0 // indirect
	github.com/whyrusleeping/cbor-gen v0.3.1 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	github.com/xanzy/ssh-agent v0.3.3 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	gitlab.com/digitalxero/go-conventional-commit v1.0.7 // indirect
	gitlab.com/gitlab-org/api/client-go v1.6.0 // indirect
	go.mongodb.org/mongo-driver v1.17.6 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.39.0 // indirect
	go.opentelemetry.io/otel/sdk v1.42.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.42.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	gocloud.dev v0.44.0 // indirect
	golang.org/x/term v0.41.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	gopkg.in/alexcesaro/quotedprintable.v3 v3.0.0-20150716171945-2caba252f4dc // indirect
	gopkg.in/mail.v2 v2.3.1 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	k8s.io/klog/v2 v2.140.0 // indirect
	lukechampine.com/blake3 v1.4.1 // indirect
	sigs.k8s.io/kind v0.30.0 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
	software.sslmate.com/src/go-pkcs12 v0.6.0 // indirect
)

require (
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/aiplatform v1.120.0
	cloud.google.com/go/auth v0.18.2
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.5.3 // indirect
	cloud.google.com/go/longrunning v0.8.0 // indirect
	github.com/BurntSushi/toml v1.5.0 // indirect
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/RoaringBitmap/roaring/v2 v2.16.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.4 // indirect
	github.com/blevesearch/geo v0.2.5 // indirect
	github.com/blevesearch/go-faiss v1.0.28 // indirect
	github.com/blevesearch/go-porterstemmer v1.0.3 // indirect
	github.com/blevesearch/gtreap v0.1.1 // indirect
	github.com/blevesearch/mmap-go v1.2.0 // indirect
	github.com/blevesearch/scorch_segment_api/v2 v2.4.3 // indirect
	github.com/blevesearch/segment v0.9.1 // indirect
	github.com/blevesearch/snowballstem v0.9.0 // indirect
	github.com/blevesearch/upsidedown_store_api v1.0.2 // indirect
	github.com/blevesearch/vellum v1.2.0 // indirect
	github.com/blevesearch/zapx/v11 v11.4.3 // indirect
	github.com/blevesearch/zapx/v12 v12.4.3 // indirect
	github.com/blevesearch/zapx/v13 v13.4.3 // indirect
	github.com/blevesearch/zapx/v14 v14.4.3 // indirect
	github.com/blevesearch/zapx/v15 v15.4.3 // indirect
	github.com/blevesearch/zapx/v16 v16.3.1 // indirect
	github.com/cockroachdb/crlib v0.0.0-20251122031428-fe658a2dbda1 // indirect
	github.com/cockroachdb/errors v1.12.0 // indirect
	github.com/cockroachdb/logtags v0.0.0-20241215232642-bb51bb14a506 // indirect
	github.com/cockroachdb/redact v1.1.8 // indirect
	github.com/cockroachdb/swiss v0.0.0-20251224182025-b0f6560f979b // indirect
	github.com/cockroachdb/tokenbucket v0.0.0-20250429170803-42689b6311bb // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/getsentry/sentry-go v0.43.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.14 // indirect
	github.com/googleapis/gax-go/v2 v2.19.0 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.21 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.67.5 // indirect
	github.com/prometheus/procfs v0.20.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	github.com/spf13/afero v1.15.0 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	github.com/spf13/cobra v1.10.2
	github.com/spf13/pflag v1.0.10
	github.com/spf13/viper v1.21.0
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/tinylru v1.2.1 // indirect
	github.com/tidwall/wal v1.2.1
	github.com/xiang90/probing v0.0.0-20221125231312-a49e3df8f510
	go.etcd.io/bbolt v1.4.3 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.67.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.67.0 // indirect
	go.opentelemetry.io/otel v1.42.0 // indirect
	go.opentelemetry.io/otel/metric v1.42.0 // indirect
	go.opentelemetry.io/otel/trace v1.42.0 // indirect
	go.uber.org/multierr v1.11.0
	golang.org/x/crypto v0.49.0
	golang.org/x/mod v0.34.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	golang.org/x/time v0.15.0
	golang.org/x/tools v0.43.0 // indirect
	google.golang.org/api v0.272.0
	google.golang.org/genproto v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260319201613-d00831a3d3e7 // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

tool (
	github.com/dprotaso/go-yit
	github.com/goreleaser/goreleaser/v2
	github.com/ipfs/go-log/v2
	github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen
	github.com/wagoodman/go-progress
	golang.org/x/tools/cmd/goimports
)
