package sql

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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

func Test_parseDBConnectionString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    *DB
		wantErr bool
	}{
		{
			name: "should pass",
			args: args{
				s: "mysql://username:password@host:3306/dbname?query=pizza&multiStatements=true",
			},
			want: &DB{
				DBSource: DBSource_mysql,
				User:     "username",
				Password: "password",
				Hosts:    []string{"host"},
				Port:     "3306",
				DBName:   "dbname",
				RawQuery: "query=pizza&multiStatements=true",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDBConnectionString(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseDBConnectionString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			opts := []cmp.Option{
				cmpopts.IgnoreUnexported(DB{}),
			}

			if !cmp.Equal(got, tt.want, opts...) {
				t.Fatal(cmp.Diff(got, tt.want, opts...))
			}
		})
	}
}

func TestDB_getPostgresDataSource(t *testing.T) {

	tests := []struct {
		name    string
		db      *DB
		want    string
		wantErr bool
	}{
		{
			name: "should pass; with mutli statement",
			db: func() *DB {
				db, err := parseDBConnectionString("postgres://username:password@host:5432/dbname?query=pizza&x-multi-statements=false")
				if err != nil {
					t.Fatal(err)
				}
				return db
			}(),
			want: "postgres://username:password@host:5432/dbname?sslmode=disable&query=pizza&x-multi-statements=false",
		},
		{
			name: "should pass; without mutli statement",
			db: func() *DB {
				db, err := parseDBConnectionString("postgres://username:password@host:5432/dbname?query=pizza")
				if err != nil {
					t.Fatal(err)
				}
				return db
			}(),
			want: "postgres://username:password@host:5432/dbname?sslmode=disable&query=pizza&x-multi-statements=true",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			got, err := tt.db.getPostgresDataSource()
			if (err != nil) != tt.wantErr {
				t.Errorf("DB.getPostgresDataSource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DB.getPostgresDataSource() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_getMysqlDataSource(t *testing.T) {

	tests := []struct {
		name    string
		db      *DB
		want    string
		wantErr bool
	}{
		{
			name: "should pass; with multi statement",
			db: func() *DB {
				db, err := parseDBConnectionString("mysql://username:password@host:3306/dbname?query=pizza&multiStatements=false")
				if err != nil {
					t.Fatal(err)
				}
				return db
			}(),
			want: "username:password@tcp(host:3306)/dbname?query=pizza&multiStatements=false",
		},
		{
			name: "should pass; without mutli statement",
			db: func() *DB {
				db, err := parseDBConnectionString("mysql://username:password@host:3306/dbname?query=pizza")
				if err != nil {
					t.Fatal(err)
				}
				return db
			}(),
			want: "username:password@tcp(host:3306)/dbname?query=pizza&multiStatements=true",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			got, err := tt.db.getMysqlDataSource()
			if (err != nil) != tt.wantErr {
				t.Errorf("DB.getMysqlDataSource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DB.getMysqlDataSource() = %v, want %v", got, tt.want)
			}
		})
	}
}
