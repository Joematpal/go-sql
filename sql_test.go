package sql

import (
	"testing"

	_ "github.com/golang-migrate/migrate/source/file"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestToNamedStatement(t *testing.T) {
	type args struct {
		dbSource DBSource
		stmt     string
		names    []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "should pass for a postgres statement",
			args: args{
				dbSource: "postgres",
				stmt:     "SELECT * FROM test WHERE id = $1 AND name = $2",
				names:    []string{"id", "name"},
			},
			want: "SELECT * FROM test WHERE id = :id AND name = :name",
		},
		{
			name: "should pass for a mysql statement",
			args: args{
				dbSource: "mysql",
				stmt:     "SELECT * FROM test WHERE id = ? AND name = ?",
				names:    []string{"id", "name"},
			},
			want: "SELECT * FROM test WHERE id = :id AND name = :name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ToNamedStatement(tt.args.dbSource, tt.args.stmt, tt.args.names); got != tt.want {
				t.Errorf("ToNamedStatement() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNew(t *testing.T) {
	type args struct {
		in []Option
	}
	tests := []struct {
		name    string
		args    args
		want    *DB
		wantErr bool
	}{
		{
			name: "cql connection with migrate false",
			args: args{
				in: []Option{
					WithType("cql"),
					WithDBName("test_db"),
					WithHost("127.0.0.1"),
					WithUser("cassandra"),
					WithPassword("cassandra"),
					WithPort("9042"),
					WithMigrate(true),
					WithMigratePath("database/cql"),
				},
			},
			want: &DB{
				DBSource:    DBSource_cql,
				User:        "cassandra",
				Password:    "cassandra",
				Port:        "9042",
				DBName:      "test_db",
				Hosts:       []string{"127.0.0.1"},
				Migrate:     true,
				MigratePath: "database/cql",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.args.in...)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			opts := []cmp.Option{cmpopts.IgnoreUnexported(DB{}), cmpopts.IgnoreFields(DB{}, "sql", "cql")}
			if !cmp.Equal(got, tt.want, opts...) {
				t.Error(cmp.Diff(got, tt.want, opts...))
			}
			if tt.want.DBSource == DBSource_cql {
				session, err := got.CQLX()
				if err != nil {
					t.Fatalf("cqlx: %v", err)
				}
				if err := session.ExecStmt("SELECT cql_version FROM system.local"); err != nil {
					t.Fatalf("ping: %v", err)
				}
			} else {

			}

		})
	}
}
