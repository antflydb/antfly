package common

import "testing"

func TestValidateBackupID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{name: "simple", id: "backup-123"},
		{name: "uuid", id: "550e8400-e29b-41d4-a716-446655440000"},
		{name: "dot underscore", id: "backup_2026.05.20"},
		{name: "empty", id: "", wantErr: true},
		{name: "slash", id: "../etc/passwd", wantErr: true},
		{name: "backslash", id: `..\\windows`, wantErr: true},
		{name: "traversal substring", id: "backup..evil", wantErr: true},
		{name: "space", id: "backup id", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBackupID(tt.id)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateBackupID(%q) err=%v, wantErr=%v", tt.id, err, tt.wantErr)
			}
		})
	}
}
