module github.com/antflydb/antfly/examples/screenshots-shots-shots

go 1.26

replace (
	github.com/antflydb/antfly/pkg/client => ../../pkg/client
	github.com/antflydb/antfly/pkg/libaf => ../../pkg/libaf
)

require github.com/antflydb/antfly/pkg/client v0.0.0-20260207183149-045031239f8a

require (
	github.com/antflydb/antfly/pkg/libaf v0.0.1 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/getkin/kin-openapi v0.133.0 // indirect
	github.com/go-json-experiment/json v0.0.0-20260214004413-d219187c3433 // indirect
	github.com/go-openapi/jsonpointer v0.22.5 // indirect
	github.com/go-openapi/swag/jsonname v0.25.5 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/kaptinlin/go-i18n v0.2.12 // indirect
	github.com/kaptinlin/jsonpointer v0.4.17 // indirect
	github.com/kaptinlin/jsonschema v0.7.6 // indirect
	github.com/kaptinlin/messageformat-go v0.4.18 // indirect
	github.com/mailru/easyjson v0.9.2 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/oapi-codegen/runtime v1.3.0 // indirect
	github.com/oasdiff/yaml v0.0.0-20250309154309-f31be36b4037 // indirect
	github.com/oasdiff/yaml3 v0.0.0-20250309153720-d2182401db90 // indirect
	github.com/perimeterx/marshmallow v1.1.5 // indirect
	github.com/woodsbury/decimal128 v1.4.0 // indirect
	golang.org/x/text v0.35.0 // indirect
)
