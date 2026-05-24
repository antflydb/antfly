package s3

import "testing"

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		name         string
		endpoint     string
		useSsl       bool
		wantEndpoint string
		wantSecure   bool
	}{
		{
			name:         "plain hostname uses provided useSsl=true",
			endpoint:     "s3.amazonaws.com",
			useSsl:       true,
			wantEndpoint: "s3.amazonaws.com",
			wantSecure:   true,
		},
		{
			name:         "plain hostname uses provided useSsl=false",
			endpoint:     "localhost:9000",
			useSsl:       false,
			wantEndpoint: "localhost:9000",
			wantSecure:   false,
		},
		{
			name:         "https URL strips scheme and uses https=true",
			endpoint:     "https://storage.googleapis.com",
			useSsl:       false, // should be overridden by scheme
			wantEndpoint: "storage.googleapis.com",
			wantSecure:   true,
		},
		{
			name:         "http URL strips scheme and uses http=false",
			endpoint:     "http://localhost:9000",
			useSsl:       true, // should be overridden by scheme
			wantEndpoint: "localhost:9000",
			wantSecure:   false,
		},
		{
			name:         "https URL with port",
			endpoint:     "https://s3.example.com:9000",
			useSsl:       false,
			wantEndpoint: "s3.example.com:9000",
			wantSecure:   true,
		},
		{
			name:         "https URL with path (path ignored)",
			endpoint:     "https://storage.googleapis.com/some/path",
			useSsl:       false,
			wantEndpoint: "storage.googleapis.com",
			wantSecure:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotEndpoint, gotSecure := parseEndpoint(tt.endpoint, tt.useSsl)
			if gotEndpoint != tt.wantEndpoint {
				t.Errorf("parseEndpoint() endpoint = %q, want %q", gotEndpoint, tt.wantEndpoint)
			}
			if gotSecure != tt.wantSecure {
				t.Errorf("parseEndpoint() secure = %v, want %v", gotSecure, tt.wantSecure)
			}
		})
	}
}
