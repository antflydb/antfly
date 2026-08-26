package controllers

import "testing"

func TestNormalizeClusterDomain(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "empty uses Kubernetes default", want: DefaultClusterDomain},
		{name: "trims whitespace and root dot", input: "  corp.internal.  ", want: "corp.internal"},
		{name: "rejects empty root", input: ".", wantErr: true},
		{name: "rejects multiple root dots", input: "corp.internal..", wantErr: true},
		{name: "rejects uppercase", input: "Corp.internal", wantErr: true},
		{name: "rejects invalid label", input: "corp_internal", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeClusterDomain(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NormalizeClusterDomain(%q) succeeded, want error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeClusterDomain(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("NormalizeClusterDomain(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestInternalDNSNames(t *testing.T) {
	if got, want := serviceDNSName("antfly-public-api", "database", "corp.internal"), "antfly-public-api.database.svc.corp.internal"; got != want {
		t.Fatalf("serviceDNSName() = %q, want %q", got, want)
	}
	if got, want := statefulPodDNSName("antfly-data-2", "antfly-data", "database", "corp.internal"), "antfly-data-2.antfly-data.database.svc.corp.internal"; got != want {
		t.Fatalf("statefulPodDNSName() = %q, want %q", got, want)
	}
}

func TestNormalizeClusterDomainField(t *testing.T) {
	domain := " corp.internal. "
	if err := normalizeClusterDomainField(&domain); err != nil {
		t.Fatalf("normalizeClusterDomainField(): %v", err)
	}
	if domain != "corp.internal" {
		t.Fatalf("normalized domain = %q, want corp.internal", domain)
	}

	invalid := "invalid_domain"
	if err := normalizeClusterDomainField(&invalid); err == nil {
		t.Fatal("normalizeClusterDomainField() succeeded for invalid domain")
	}
}
