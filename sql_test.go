package sql

import (
	"testing"

	"github.com/digital-dream-labs/go-sql/v2/table"
	test_users "github.com/digital-dream-labs/go-sql/v2/test/users"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	cqlreflectx "github.com/scylladb/go-reflectx"
	"github.com/scylladb/gocqlx/v2/qb"
)

var testUserTable = table.New("users", map[string]struct{}{
	"userId":    {},
	"username":  {},
	"email":     {},
	"verified":  {},
	"createdAt": {},
	"updatedAt": {},
	"createdBy": {},
	"updatedBy": {},
})

var testUserSettingsTable = table.New("user_settings", map[string]struct{}{
	"userId":    {},
	"metadata":  {},
	"scope":     {},
	"createdAt": {},
	"updatedAt": {},
	"createdBy": {},
	"updatedBy": {},
})

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
		// {
		// 	name: "should pass; cql connection with migrate",
		// 	args: args{
		// 		in: []Option{
		// 			WithType("cql"),
		// 			WithDBName("test_db"),
		// 			WithHost("127.0.0.1"),
		// 			WithUser("cassandra"),
		// 			WithPassword("cassandra"),
		// 			WithPort("9042"),
		// 			WithMigrate(true),
		// 			WithMigratePath("database/cql"),
		// 		},
		// 	},
		// 	want: &DB{
		// 		DBSource:    DBSource_cql,
		// 		User:        "cassandra",
		// 		Password:    "cassandra",
		// 		Port:        "9042",
		// 		DBName:      "test_db",
		// 		Hosts:       []string{"127.0.0.1"},
		// 		Migrate:     true,
		// 		MigratePath: "database/cql",
		// 	},
		// },
		// {
		// 	name: "should pass; cql connection without migrate",
		// 	args: args{
		// 		in: []Option{
		// 			WithType("cql"),
		// 			WithDBName("test_db"),
		// 			WithHost("127.0.0.1"),
		// 			WithUser("cassandra"),
		// 			WithPassword("cassandra"),
		// 			WithPort("9042"),
		// 		},
		// 	},
		// 	want: &DB{
		// 		DBSource:    DBSource_cql,
		// 		User:        "cassandra",
		// 		Password:    "cassandra",
		// 		Port:        "9042",
		// 		DBName:      "test_db",
		// 		Hosts:       []string{"127.0.0.1"},
		// 		MigratePath: "database/sql",
		// 	},
		// },
		// {
		// 	name: "should pass; postgres connection with migrate",
		// 	args: args{
		// 		in: []Option{
		// 			WithType("postgres"),
		// 			WithDBName("test_db"),
		// 			WithHost("127.0.0.1"),
		// 			WithUser("postgres"),
		// 			WithPassword("postgres"),
		// 			WithPort("5432"),
		// 			WithMigrate(true),
		// WithMigratePath("database/psql"),
		// 		},
		// 	},
		// 	want: &DB{
		// 		DBSource:    DBSource_postgres,
		// 		User:        "postgres",
		// 		Password:    "postgres",
		// 		Port:        "5432",
		// 		DBName:      "test_db",
		// 		Hosts:       []string{"127.0.0.1"},
		// 		Migrate:     true,
		// 		MigratePath: "database/psql",
		// 	},
		// },
		// {
		// 	name: "should pass; postgres connection without migrate",
		// 	args: args{
		// 		in: []Option{
		// 			WithType("postgres"),
		// 			WithDBName("test_db"),
		// 			WithHost("127.0.0.1"),
		// 			WithUser("postgres"),
		// 			WithPassword("postgres"),
		// 			WithPort("5432"),
		// 		},
		// 	},
		// 	want: &DB{
		// 		DBSource:    DBSource_postgres,
		// 		User:        "postgres",
		// 		Password:    "postgres",
		// 		Port:        "5432",
		// 		DBName:      "test_db",
		// 		Hosts:       []string{"127.0.0.1"},
		// 		MigratePath: "database/sql",
		// 	},
		// },
		{
			name: "should pass; mysql connection with migrate",
			args: args{
				in: []Option{
					WithType("mysql"),
					WithDBName("test_db"),
					WithHost("127.0.0.1"),
					WithUser("mysql"),
					WithPassword("mysql"),
					WithPort("3306"),
					WithMigrate(true),
					WithMigratePath("database/mysql"),
				},
			},
			want: &DB{
				DBSource:    DBSource_mysql,
				User:        "mysql",
				Password:    "mysql",
				Port:        "3306",
				DBName:      "test_db",
				Hosts:       []string{"127.0.0.1"},
				Migrate:     true,
				MigratePath: "database/mysql",
			},
		},
		{
			name: "should pass; mysql connection without migrate",
			args: args{
				in: []Option{
					WithType("mysql"),
					WithDBName("test_db"),
					WithHost("127.0.0.1"),
					WithUser("mysql"),
					WithPassword("mysql"),
					WithPort("3306"),
				},
			},
			want: &DB{
				DBSource:    DBSource_mysql,
				User:        "mysql",
				Password:    "mysql",
				Port:        "3306",
				DBName:      "test_db",
				Hosts:       []string{"127.0.0.1"},
				MigratePath: "database/sql",
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
			src, _ := got.getDataSource()
			deleteDB(src)

			defer func() {
				if err := got.DropTables("schema_migrations"); err != nil {
					t.Fatalf("flush tables: %v", err)
				}
				// if err := got.FlushAll(); err != nil {
				// 	t.Fatalf("flush tables: %v", err)
				// }
			}()

			opts := []cmp.Option{cmpopts.IgnoreUnexported(DB{}), cmpopts.IgnoreFields(DB{}, "sql", "cql")}
			if !cmp.Equal(got, tt.want, opts...) {
				t.Error(cmp.Diff(got, tt.want, opts...))
			}
			if err := got.Ping(); err != nil {
				t.Fatalf("ping: %v", err)
			}
		})
	}
}

func TestDB_SelectFromMap(t *testing.T) {

	type args struct {
		dst   *[]*test_users.User
		stmt  string
		names []string
		args  map[string]interface{}
	}
	tests := []struct {
		name             string
		fields           []Option
		args             args
		want             []*test_users.User
		camelCaseColumns bool
		wantErr          bool
	}{
		//
		//// CASSANDRA
		//
		{
			name: "should pass; cassandra connection with migrate",
			fields: []Option{
				WithType("cql"),
				WithDBName("test_db"),
				WithHosts("127.0.0.1"),
				WithUser("cassandra"),
				WithPassword("cassandra"),
				WithPort("9042"),
				WithMigrate(true),
				WithMigratePath("database/cql"),
			},
			args: args{
				dst: &[]*test_users.User{},
				args: map[string]interface{}{
					"user_id": "test_1",
				},
			},
			want: []*test_users.User{
				{
					UserID:   "test_1",
					Username: "test_username_1",
					Email:    "test_email_1",
					Verified: true,
				},
			},
		},
		{
			name: "should pass; cassandra connection without migrate",
			fields: []Option{
				WithType("cql"),
				WithDBName("test_db"),
				WithHost("127.0.0.1"),
				WithUser("cassandra"),
				WithPassword("cassandra"),
				WithPort("9042"),
			},
			args: args{
				dst: &[]*test_users.User{},
				args: map[string]interface{}{
					"user_id": "test_2",
				},
			},
			want: []*test_users.User{
				{
					UserID:   "test_2",
					Username: "test_username_2",
					Email:    "test_email_2",
					Verified: true,
				},
			},
		},
		//
		//// POSTGRES
		//
		{
			name: "should pass; postgres connection with migrate",
			fields: []Option{
				WithType("postgres"),
				WithDBName("test_db"),
				WithHost("127.0.0.1"),
				WithUser("postgres"),
				WithPassword("postgres"),
				WithPort("5432"),
				WithMigrate(true),
				WithMigratePath("database/psql"),
			},
			args: args{
				dst: &[]*test_users.User{},
				args: map[string]interface{}{
					"user_id": "c52hsocs70r9j6qad7jg",
				},
			},
			want: []*test_users.User{
				{
					UserID:   "c52hsocs70r9j6qad7jg",
					Username: "test_username_1",
					Email:    "test_email_1",
					Verified: true,
				},
			},
		},
		{
			name: "should pass; postgres connection without migrate",
			fields: []Option{
				WithType("postgres"),
				WithDBName("test_db"),
				WithHost("127.0.0.1"),
				WithUser("postgres"),
				WithPassword("postgres"),
				WithPort("5432"),
			},
			args: args{
				dst: &[]*test_users.User{},
				args: map[string]interface{}{
					"user_id": "c52hsocs70r9j6qad7jx",
				},
			},
			want: []*test_users.User{
				{
					UserID:   "c52hsocs70r9j6qad7jx",
					Username: "test_username_2",
					Email:    "test_email_2",
					Verified: true,
				},
			},
		},
		//
		//// MYSQL
		//
		{
			name: "should pass; mysql connection with migrate",
			fields: []Option{
				WithType("mysql"),
				WithDBName("test_db"),
				WithHost("127.0.0.1"),
				WithUser("mysql"),
				WithPassword("mysql"),
				WithPort("3306"),
				WithMigrate(true),
				WithMigratePath("database/mysql"),
			},
			args: args{
				dst: &[]*test_users.User{},
				args: map[string]interface{}{
					"user_id": "c52hsocs70r9j6qad7jg",
				},
			},
			want: []*test_users.User{
				{
					UserID:   "c52hsocs70r9j6qad7jg",
					Username: "test_username_1",
					Email:    "test_email_1",
					Verified: true,
				},
			},
		},
		{
			name: "should pass; mysql connection without migrate",
			fields: []Option{
				WithType("mysql"),
				WithDBName("test_db"),
				WithHost("127.0.0.1"),
				WithUser("mysql"),
				WithPassword("mysql"),
				WithPort("3306"),
			},
			args: args{
				dst: &[]*test_users.User{},
				args: map[string]interface{}{
					"user_id": "c52hsocs70r9j6qad7jx",
				},
			},
			want: []*test_users.User{
				{
					UserID:   "c52hsocs70r9j6qad7jx",
					Username: "test_username_2",
					Email:    "test_email_2",
					Verified: true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := New(tt.fields...)
			if err != nil {
				t.Fatal(err)
			}
			columns := testUserTable.ListColumns()
			if !tt.camelCaseColumns {
				columns = MapColumns(testUserTable.ListColumns(), cqlreflectx.CamelToSnakeASCII)
			}

			stmt, names := qb.Insert(testUserTable.Name).Columns(columns...).ToCql()
			for _, want := range tt.want {
				if err := o.Exec(stmt, names, want); err != nil {
					t.Fatalf("test insert: %v", err)
				}
			}

			if tt.args.stmt == "" {
				userId := "userId"
				if !tt.camelCaseColumns {
					userId = "user_id"
				}
				tt.args.stmt, tt.args.names = qb.Select(testUserTable.Name).Columns(columns...).Where(qb.Eq(userId)).ToCql()
			}

			if err := o.SelectFromMap(tt.args.dst, tt.args.stmt, tt.args.names, tt.args.args); (err != nil) != tt.wantErr {
				t.Errorf("DB.Select() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !cmp.Equal(tt.want, *tt.args.dst) {
				t.Fatalf(cmp.Diff(tt.want, *tt.args.dst))
			}
		})
		t.Cleanup(func() {
			for _, tt := range tests {
				o, err := New(tt.fields...)
				if err != nil {
					t.Fatal(err)
				}
				o.DropTables("users", "schema_migrations")
			}
		})
	}
}

func MapColumns(columns []string, mapper func(string) string) []string {
	out := make([]string, len(columns))
	for i := range columns {
		out[i] = mapper(columns[i])
	}
	return out
}

func TestDB_Select(t *testing.T) {
	type Req struct {
		UserID string `json:"user_id"`
	}
	type args struct {
		dst   *[]*test_users.UserSettings
		stmt  string
		names []string
		args  Req
	}

	tests := []struct {
		name             string
		fields           []Option
		args             args
		camelCaseColumns bool
		want             []*test_users.UserSettings
		wantErr          bool
	}{
		{
			name: "should pass; cql",
			fields: []Option{
				WithType("cql"),
				WithDBName("test_db"),
				WithHosts("127.0.0.1"),
				WithUser("cassandra"),
				WithPassword("cassandra"),
				WithPort("9042"),
				WithMigrate(true),
				WithMigratePath("database/cql"),
			},
			args: args{
				dst:  &[]*test_users.UserSettings{},
				args: Req{UserID: "test_user_44"},
			},
			want: []*test_users.UserSettings{
				{
					UserId: "test_user_44",
					Scope:  &test_users.Scope{Domain: "TEST_DOMAIN", Operation: test_users.ScopeOperation_Read},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := New(tt.fields...)
			if err != nil {
				t.Fatal(err)
			}
			columns := testUserSettingsTable.ListColumns()
			if !tt.camelCaseColumns {
				columns = MapColumns(testUserSettingsTable.ListColumns(), cqlreflectx.CamelToSnakeASCII)
			}

			stmt, names := qb.Insert(testUserSettingsTable.Name).Columns(columns...).ToCql()

			for _, want := range tt.want {
				if err := o.Exec(stmt, names, want); err != nil {
					t.Fatalf("exec: %v", err)
				}
			}

			if tt.args.stmt == "" {
				userId := "userId"
				if !tt.camelCaseColumns {
					userId = "user_id"
				}
				tt.args.stmt, tt.args.names = qb.Select(testUserSettingsTable.Name).Columns(columns...).Where(qb.Eq(userId)).ToCql()
			}

			if err := o.Select(tt.args.dst, tt.args.stmt, tt.args.names, tt.args.args); (err != nil) != tt.wantErr {
				t.Fatalf("DB.Select() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !cmp.Equal(tt.want, *tt.args.dst) {
				t.Fatalf(cmp.Diff(tt.want, *tt.args.dst))
			}

		})

		t.Cleanup(func() {
			for _, tt := range tests {
				o, err := New(tt.fields...)
				if err != nil {
					t.Fatal(err)
				}
				o.DropTables("user_settings", "schema_migrations")
			}
		})
	}
}
