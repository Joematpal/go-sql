package sql

import (
	"reflect"
	"testing"

	cqlreflectx "github.com/scylladb/go-reflectx"
)

func Test_preMapFunc(t *testing.T) {
	type args struct {
		f   func(string) string
		tag string
	}
	tests := []struct {
		name string
		args args
		want string
	}{

		{
			name: "should pass camel to snake; value: userId,omitempty",
			args: args{
				f:   cqlreflectx.CamelToSnakeASCII,
				tag: "userId,comitempty",
			},
			want: "user_id",
		},
		{
			name: "should pass camel to snake; value: userId",
			args: args{
				f:   cqlreflectx.CamelToSnakeASCII,
				tag: "userId",
			},
			want: "user_id",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := preMapFunc(tt.args.f); !reflect.DeepEqual(got(tt.args.tag), tt.want) {
				t.Errorf("preMapFunc() = %v, want %v", got(tt.args.tag), tt.want)
			}
		})
	}
}
