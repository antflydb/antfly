package docsaf

import (
	"strings"
	"testing"
)

func TestOpenAPIProcessor_CanProcess(t *testing.T) {
	op := &OpenAPIProcessor{}

	tests := []struct {
		name        string
		contentType string
		path        string
		want        bool
	}{
		{"yaml file", "", "api.yaml", true},
		{"yml file", "", "api.yml", true},
		{"json file", "", "api.json", true},
		{"YAML uppercase", "", "api.YAML", true},
		{"yaml content type", "application/x-yaml", "test", true},
		{"application/yaml", "application/yaml", "test", true},
		{"text/yaml", "text/yaml", "test", true},
		{"json content type", "application/json", "test", true},
		{"markdown file", "", "test.md", false},
		{"html file", "", "test.html", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := op.CanProcess(tt.contentType, tt.path); got != tt.want {
				t.Errorf("CanProcess(%q, %q) = %v, want %v", tt.contentType, tt.path, got, tt.want)
			}
		})
	}
}

func TestOpenAPIProcessor_Process_BasicSpec(t *testing.T) {
	op := &OpenAPIProcessor{}

	spec := []byte(`
openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"
  description: A test API
paths:
  /users:
    get:
      operationId: getUsers
      summary: Get all users
      responses:
        "200":
          description: Success
    post:
      operationId: createUser
      summary: Create a user
      responses:
        "201":
          description: Created
components:
  schemas:
    User:
      type: object
      properties:
        id:
          type: string
        name:
          type: string
`)

	sections, err := op.Process("api.yaml", "", "https://api.example.com", spec)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Should have: 1 info + 2 paths + 1 schema = 4 sections
	if len(sections) < 4 {
		t.Errorf("Expected at least 4 sections, got %d", len(sections))
	}

	// Check section types
	types := make(map[string]int)
	for _, s := range sections {
		types[s.Type]++
	}

	if types["openapi_info"] != 1 {
		t.Errorf("Expected 1 openapi_info section, got %d", types["openapi_info"])
	}
	if types["openapi_path"] != 2 {
		t.Errorf("Expected 2 openapi_path sections, got %d", types["openapi_path"])
	}
	if types["openapi_schema"] != 1 {
		t.Errorf("Expected 1 openapi_schema section, got %d", types["openapi_schema"])
	}
}

func TestOpenAPIProcessor_Process_InvalidSpec(t *testing.T) {
	op := &OpenAPIProcessor{}

	content := []byte(`not a valid openapi spec`)

	_, err := op.Process("invalid.yaml", "", "", content)
	if err == nil {
		t.Error("Expected error for invalid OpenAPI spec, got nil")
	}
}

func TestOpenAPIProcessor_ExtractQuestions_InfoLevel(t *testing.T) {
	op := &OpenAPIProcessor{}

	spec := []byte(`
openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"
  x-docsaf-questions:
    - How do I get an API key?
    - What is the rate limit?
paths: {}
`)

	questions, err := op.ExtractQuestions("api.yaml", "https://api.example.com/api.yaml", spec)
	if err != nil {
		t.Fatalf("ExtractQuestions failed: %v", err)
	}

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions from info extension, got %d", len(questions))
	}

	if questions[0].Text != "How do I get an API key?" {
		t.Errorf("Expected 'How do I get an API key?', got %q", questions[0].Text)
	}
	if questions[0].SourceType != "openapi_info" {
		t.Errorf("Expected source type 'openapi_info', got %q", questions[0].SourceType)
	}
	if questions[0].Context != "Test API" {
		t.Errorf("Expected context 'Test API', got %q", questions[0].Context)
	}
}

func TestOpenAPIProcessor_ExtractQuestions_OperationLevel(t *testing.T) {
	op := &OpenAPIProcessor{}

	spec := []byte(`
openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"
paths:
  /users:
    get:
      operationId: getUsers
      summary: Get all users
      x-docsaf-questions:
        - How do I paginate results?
        - Can I filter by status?
      responses:
        "200":
          description: Success
`)

	questions, err := op.ExtractQuestions("api.yaml", "", spec)
	if err != nil {
		t.Fatalf("ExtractQuestions failed: %v", err)
	}

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions from operation extension, got %d", len(questions))
	}

	if questions[0].Text != "How do I paginate results?" {
		t.Errorf("Expected 'How do I paginate results?', got %q", questions[0].Text)
	}
	if questions[0].SourceType != "openapi_operation" {
		t.Errorf("Expected source type 'openapi_operation', got %q", questions[0].SourceType)
	}
	if questions[0].Context != "getUsers" {
		t.Errorf("Expected context 'getUsers', got %q", questions[0].Context)
	}
}

func TestOpenAPIProcessor_ExtractQuestions_SchemaLevel(t *testing.T) {
	op := &OpenAPIProcessor{}

	spec := []byte(`
openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"
paths: {}
components:
  schemas:
    User:
      type: object
      x-docsaf-questions:
        - What fields are required?
        - How do I format the date field?
      properties:
        id:
          type: string
`)

	questions, err := op.ExtractQuestions("api.yaml", "", spec)
	if err != nil {
		t.Fatalf("ExtractQuestions failed: %v", err)
	}

	if len(questions) != 2 {
		t.Fatalf("Expected 2 questions from schema extension, got %d", len(questions))
	}

	if questions[0].Text != "What fields are required?" {
		t.Errorf("Expected 'What fields are required?', got %q", questions[0].Text)
	}
	if questions[0].SourceType != "openapi_schema" {
		t.Errorf("Expected source type 'openapi_schema', got %q", questions[0].SourceType)
	}
	if questions[0].Context != "User" {
		t.Errorf("Expected context 'User', got %q", questions[0].Context)
	}
}

func TestOpenAPIProcessor_ExtractQuestions_PathLevel(t *testing.T) {
	op := &OpenAPIProcessor{}

	spec := []byte(`
openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"
paths:
  /users/{id}:
    x-docsaf-questions:
      - What happens if the user doesn't exist?
    get:
      summary: Get user by ID
      responses:
        "200":
          description: Success
`)

	questions, err := op.ExtractQuestions("api.yaml", "", spec)
	if err != nil {
		t.Fatalf("ExtractQuestions failed: %v", err)
	}

	if len(questions) != 1 {
		t.Fatalf("Expected 1 question from path extension, got %d", len(questions))
	}

	if questions[0].SourceType != "openapi_path" {
		t.Errorf("Expected source type 'openapi_path', got %q", questions[0].SourceType)
	}
	if questions[0].Context != "/users/{id}" {
		t.Errorf("Expected context '/users/{id}', got %q", questions[0].Context)
	}
}

func TestOpenAPIProcessor_ExtractQuestions_MultipleLocations(t *testing.T) {
	op := &OpenAPIProcessor{}

	spec := []byte(`
openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"
  x-docsaf-questions:
    - What authentication methods are supported?
paths:
  /users:
    x-docsaf-questions:
      - How do I list all users?
    get:
      operationId: getUsers
      x-docsaf-questions:
        - Can I filter results?
      responses:
        "200":
          description: Success
components:
  schemas:
    User:
      type: object
      x-docsaf-questions:
        - What is the User schema?
`)

	questions, err := op.ExtractQuestions("api.yaml", "", spec)
	if err != nil {
		t.Fatalf("ExtractQuestions failed: %v", err)
	}

	// Should have: 1 info + 1 path + 1 operation + 1 schema = 4 questions
	if len(questions) != 4 {
		t.Fatalf("Expected 4 questions from multiple locations, got %d", len(questions))
	}

	// Check all source types are represented
	sourceTypes := make(map[string]int)
	for _, q := range questions {
		sourceTypes[q.SourceType]++
	}

	if sourceTypes["openapi_info"] != 1 {
		t.Errorf("Expected 1 openapi_info question, got %d", sourceTypes["openapi_info"])
	}
	if sourceTypes["openapi_path"] != 1 {
		t.Errorf("Expected 1 openapi_path question, got %d", sourceTypes["openapi_path"])
	}
	if sourceTypes["openapi_operation"] != 1 {
		t.Errorf("Expected 1 openapi_operation question, got %d", sourceTypes["openapi_operation"])
	}
	if sourceTypes["openapi_schema"] != 1 {
		t.Errorf("Expected 1 openapi_schema question, got %d", sourceTypes["openapi_schema"])
	}
}

func TestOpenAPIProcessor_ExtractQuestions_NoQuestions(t *testing.T) {
	op := &OpenAPIProcessor{}

	spec := []byte(`
openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"
paths:
  /users:
    get:
      summary: Get users
      responses:
        "200":
          description: Success
`)

	questions, err := op.ExtractQuestions("api.yaml", "", spec)
	if err != nil {
		t.Fatalf("ExtractQuestions failed: %v", err)
	}

	if len(questions) != 0 {
		t.Errorf("Expected 0 questions when no x-docsaf-questions, got %d", len(questions))
	}
}

func TestOpenAPIProcessor_ExtractQuestions_ObjectFormat(t *testing.T) {
	op := &OpenAPIProcessor{}

	spec := []byte(`
openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"
  x-docsaf-questions:
    - text: How do I authenticate?
      category: auth
      priority: high
paths: {}
`)

	questions, err := op.ExtractQuestions("api.yaml", "", spec)
	if err != nil {
		t.Fatalf("ExtractQuestions failed: %v", err)
	}

	if len(questions) != 1 {
		t.Fatalf("Expected 1 question, got %d", len(questions))
	}

	if questions[0].Text != "How do I authenticate?" {
		t.Errorf("Expected 'How do I authenticate?', got %q", questions[0].Text)
	}
	if questions[0].Metadata["category"] != "auth" {
		t.Errorf("Expected metadata category 'auth', got %v", questions[0].Metadata["category"])
	}
	if questions[0].Metadata["priority"] != "high" {
		t.Errorf("Expected metadata priority 'high', got %v", questions[0].Metadata["priority"])
	}
}

func TestOpenAPIProcessor_ExtractQuestions_InvalidSpec(t *testing.T) {
	op := &OpenAPIProcessor{}

	content := []byte(`not valid openapi`)

	_, err := op.ExtractQuestions("invalid.yaml", "", content)
	if err == nil {
		t.Error("Expected error for invalid OpenAPI spec, got nil")
	}
}

func TestOpenAPIProcessor_Process_OperationWithoutID(t *testing.T) {
	op := &OpenAPIProcessor{}

	spec := []byte(`
openapi: "3.0.0"
info:
  title: Test API
  version: "1.0"
paths:
  /users:
    get:
      summary: Get users
      responses:
        "200":
          description: Success
`)

	sections, err := op.Process("api.yaml", "", "", spec)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// Find the path section
	for _, s := range sections {
		if s.Type == "openapi_path" {
			if !strings.Contains(s.Title, "GET /users") {
				t.Errorf("Expected title to contain 'GET /users', got %q", s.Title)
			}
			// Operation ID in metadata should be auto-generated
			opID, ok := s.Metadata["operation_id"].(string)
			if !ok || opID == "" {
				t.Errorf("Expected non-empty operation_id in metadata")
			}
		}
	}
}

func TestOrderedMapToMap(t *testing.T) {
	// Test with nil map
	result := orderedMapToMap(nil)
	if result != nil {
		t.Errorf("Expected nil result for nil input, got %v", result)
	}
}
