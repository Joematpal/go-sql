package table

import (
	"reflect"
	"sort"
	"testing"
)

func TestTable_OmitColumns(t *testing.T) {
	type fields struct {
		name    string
		columns columns
	}
	type args struct {
		omits []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{
			name: "should pass",
			fields: fields{
				name: "test_table",
				columns: map[string]struct{}{
					"xid":  {},
					"name": {},
					"dob":  {},
					"pass": {},
				},
			},
			args: args{
				omits: []string{
					"dob", "pass",
				},
			},
			want: []string{"xid", "name"},
		},
		{
			name: "should pass; more args that in columns",
			fields: fields{
				name: "test_table",
				columns: map[string]struct{}{
					"xid":  {},
					"name": {},
					"dob":  {},
					"pass": {},
				},
			},
			args: args{
				omits: []string{
					"dob", "pass", "stuff", "1", "blah",
				},
			},
			want: []string{"xid", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := Table{
				Name:    tt.fields.name,
				Columns: tt.fields.columns,
			}
			if got := tr.OmitColumns(tt.args.omits...); !reflect.DeepEqual(sort.StringSlice(got), sort.StringSlice(tt.want)) {
				t.Errorf("Table.OmitColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTable_ListColumns(t *testing.T) {
	type fields struct {
		name    string
		columns columns
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{
			name: "should pass",
			fields: fields{
				name: "test_table",
				columns: map[string]struct{}{
					"xid":  {},
					"name": {},
					"dob":  {},
					"pass": {},
				},
			},

			want: []string{"xid", "name", "dob", "pass"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := Table{
				Name:    tt.fields.name,
				Columns: tt.fields.columns,
			}
			if got := tr.ListColumns(); !reflect.DeepEqual(sort.StringSlice(got), sort.StringSlice(tt.want)) {
				t.Errorf("Table.ListColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}
