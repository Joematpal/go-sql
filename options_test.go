package sql

import (
	"testing"
)

func TestOptions_GetMigratePath(t *testing.T) {

	tests := []struct {
		name string
		opts *Options
		want string
	}{
		{
			name: "should pass: no protocol string",
			opts: &Options{
				MigratePath: "some/file/path",
			},
			want: "file://some/file/path",
		},
		{
			name: "should pass: has protocol string",
			opts: &Options{
				MigratePath: "github://some/file/path",
			},
			want: "github://some/file/path",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.opts.GetMigratePath(); got != tt.want {
				t.Errorf("Options.GetMigratePath() = %v, want %v", got, tt.want)
			}
		})
	}
}
