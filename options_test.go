package sql

import (
	"testing"
)

func TestDB_GetMigratePath(t *testing.T) {

	tests := []struct {
		name string
		opts *DB
		want string
	}{
		{
			name: "should pass: no protocol string",
			opts: &DB{
				MigratePath: "some/file/path",
			},
			want: "file://some/file/path",
		},
		{
			name: "should pass: has protocol string",
			opts: &DB{
				MigratePath: "github://some/file/path",
			},
			want: "github://some/file/path",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.GetMigratePath(); got != tt.want {
				t.Errorf("DB.GetMigratePath() = %v, want %v", got, tt.want)
			}
		})
	}
}
