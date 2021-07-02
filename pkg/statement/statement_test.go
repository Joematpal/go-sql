package statement

import "testing"

func TestToNamed(t *testing.T) {
	type args struct {
		dbType string
		stmt   string
		names  []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should pass for a postgres statement",
			args: args{
				dbType: "postgres",
				stmt:   "SELECT * FROM test WHERE id = $1 AND name = $2",
				names:  []string{"id", "name"},
			},
			want: "SELECT * FROM test WHERE id = :id AND name = :name",
		},
		{
			name: "should pass for a mysql statement",
			args: args{
				dbType: "mysql",
				stmt:   "SELECT * FROM test WHERE id = ? AND name = ?",
				names:  []string{"id", "name"},
			},
			want: "SELECT * FROM test WHERE id = :id AND name = :name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToNamed(tt.args.dbType, tt.args.stmt, tt.args.names); got != tt.want {
				t.Errorf("ToNamed() = %v, want %v", got, tt.want)
			}
		})
	}
}
