package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/gocql/gocql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
	cqlreflectx "github.com/scylladb/go-reflectx"
	"github.com/scylladb/gocqlx/v2"
)

type DB struct {
	sql         *sqlx.DB
	AppEnv      string   `json:"appEnv"`
	User        string   `json:"user"`
	Hosts       []string `json:"hosts"`
	DBName      string   `json:"dbName"`
	Password    string   `json:"-"`
	Port        string   `json:"port"`
	Migrate     bool     `json:"migrate"`
	MigratePath string   `json:"migratePath"`
	DBSource    DBSource `json:"dbSource"`
	Debugger    Debugger `json:"-"`
	err         error
	cql         *gocqlx.Session
	mapFunc     func(string) string
	tagMapFunc  func(string) string

	Timeout        time.Duration
	ConnectTimeout time.Duration

	RawQuery string `json:"rawQuery"`

	// CQL
	Authenticator            gocql.Authenticator `json:"-"`
	DisableInitialHostLookup bool                `json:"disableInitialHostLookup"`
	Consistency              gocql.Consistency

	// SSL
	CaPath string `json:"caPath"`
}

type DBSource string

const (
	DBSource_postgres DBSource = "postgres"
	DBSource_mysql    DBSource = "mysql"
	DBSource_cql      DBSource = "cql"
	production        string   = "production"
	development       string   = "development"
)

func (s DBSource) String() string {
	return string(s)
}

// Here it converts json that is camel case to snakecase
// Can pass through
func New(in ...Option) (*DB, error) {
	opts := &DB{
		AppEnv:      production,
		MigratePath: "database/sql",
		mapFunc:     cqlreflectx.CamelToSnakeASCII,
		tagMapFunc:  cqlreflectx.CamelToSnakeASCII,
	}
	for _, opt := range in {
		if err := opt.applyOption(opts); err != nil {
			return nil, err
		}
	}
	return opts, opts.IsValid()
}

func (o *DB) SQLX() (*sqlx.DB, error) {
	switch o.DBSource {
	case DBSource_mysql, DBSource_postgres:
		return o.sql, nil
	}

	return nil, errors.New("sql is not currently configured")
}

func (o *DB) CQLX() (*gocqlx.Session, error) {
	if o.DBSource != DBSource_cql {
		return nil, errors.New("cql is not currently configured")
	}

	return o.cql, nil
}

func (o *DB) Select(dst interface{}, stmt string, names []string, args interface{}) error {
	switch o.DBSource {
	case DBSource_postgres, DBSource_mysql:
		namedStmt := ToNamedStatement(o.DBSource, stmt, names)

		query, err := o.sql.PrepareNamed(namedStmt)
		if err != nil {
			return fmt.Errorf("prepare named: %w", err)
		}
		return query.Select(dst, args)
	case DBSource_cql:
		if val, ok := args.(map[string]interface{}); ok {
			return o.cql.Query(stmt, names).BindMap(val).Select(dst)
		} else {
			return o.cql.Query(stmt, names).BindStruct(args).Select(dst)
		}
	}
	return nil
}

// Deprecated
func (o *DB) SelectFromMap(dst interface{}, stmt string, names []string, args map[string]interface{}) error {
	switch o.DBSource {
	case DBSource_postgres, DBSource_mysql:
		namedStmt := ToNamedStatement(o.DBSource, stmt, names)

		query, err := o.sql.PrepareNamed(namedStmt)
		if err != nil {
			return fmt.Errorf("prepare named: %w", err)
		}
		defer query.Close()
		return query.Select(dst, args)
	case DBSource_cql:
		return o.cql.Query(stmt, names).BindMap(args).Select(dst)
	}
	return nil
}

// Returns one document
func (o *DB) Get(dst interface{}, stmt string, names []string, args interface{}) error {
	switch o.DBSource {
	case DBSource_postgres, DBSource_mysql:
		query, err := o.sql.PrepareNamed(ToNamedStatement(o.DBSource, stmt, names))
		if err != nil {
			return fmt.Errorf("prepare named: %w", err)
		}
		return query.Get(dst, args)
	case DBSource_cql:
		if val, ok := args.(map[string]interface{}); ok {
			return o.cql.Query(stmt, names).BindMap(val).Get(dst)
		} else {
			return o.cql.Query(stmt, names).BindStruct(args).Get(dst)
		}
	}
	return nil
}

// Deprecated
func (o *DB) GetFromMap(dst interface{}, stmt string, names []string, args map[string]interface{}) error {
	switch o.DBSource {
	case DBSource_postgres, DBSource_mysql:
		query, err := o.sql.PrepareNamed(ToNamedStatement(o.DBSource, stmt, names))
		if err != nil {
			return fmt.Errorf("prepare named: %w", err)
		}
		return query.Get(dst, args)
	case DBSource_cql:

		return o.cql.Query(stmt, names).BindMap(args).Get(dst)
	}
	return nil
}

func (o *DB) Ping() error {
	if o.cql != nil {
		return o.cql.ExecStmt("SELECT cql_version FROM system.local")
	}
	if o.sql != nil {
		return o.sql.Ping()
	}
	return errors.New("no source configured")
}

// Should be used for testing
// need to ensure that foreign key constraints are cleared in order
func (o *DB) DeleteAllRows(tableNames ...string) error {
	if o.cql != nil {
		for _, name := range tableNames {
			query := fmt.Sprintf("truncate %s", name)
			if err := o.ExecStmt(query); err != nil {
				return err
			}
		}
	}

	if o.sql != nil {
		for _, name := range tableNames {
			query := fmt.Sprintf("delete FROM %s", name)
			if err := o.ExecStmt(query); err != nil {
				return err
			}
		}
	}

	return nil
}

func (o *DB) DropTables(tableNames ...string) error {
	for _, name := range tableNames {
		query := fmt.Sprintf("drop table if exists %s", name)
		if err := o.ExecStmt(query); err != nil {
			return err
		}
	}
	return nil
}

func (o *DB) DropAll() error {
	tableNames := []string{}

	// mysql
	// select TABLE_NAME from information_schema.tables where TABLE_SCHEMA='test_db';
	if err := o.SelectFromMap(&tableNames, `select TABLE_NAME from information_schema.tables where TABLE_SCHEMA=?`, []string{"tableSchema"}, map[string]interface{}{
		"tableSchema": o.DBName,
	}); err != nil {
		return fmt.Errorf("select: %w", err)
	}
	return errors.New("no source configured")
}

func (o *DB) WriteBatch(queries []string, namesForSrcs [][]string, srcs []interface{}, opts ...BatchOption) error {
	bOpts := &BatchOptions{
		BatchType: gocql.LoggedBatch,
	}
	for _, opt := range opts {
		if err := opt.applyOption(bOpts); err != nil {
			return err
		}
	}

	if len(queries) != len(srcs) && len(queries) != len(namesForSrcs) {
		return errors.New("queries, namesForSrcs, and src  sources must match in length")
	}

	if o.cql != nil {
		batch := o.cql.Session.NewBatch(bOpts.BatchType)
		for i, query := range queries {
			var args []interface{}
			// Set Args
			for _, name := range namesForSrcs[i] {
				val := o.cql.Mapper.FieldByName(reflect.ValueOf(srcs[i]), name).Interface()
				args = append(args, val)
			}
			batch.Query(query, args...)
		}
		if err := o.cql.Session.ExecuteBatch(batch); err != nil {
			return fmt.Errorf("execute batch: %v", err)
		}
	}

	if o.sql != nil {
		tx, err := o.sql.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelDefault})
		if err != nil {
			return err
		}
		for i, query := range queries {
			var args []interface{}
			for _, name := range namesForSrcs[i] {
				args = append(args, o.sql.Mapper.FieldByName(reflect.ValueOf(srcs[i]), name).Interface())
			}
			if _, err := tx.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
				return fmt.Errorf("exec foriegn: %v", err)
			}
			q := FromQueryBuilder(o.DBSource, query)
			if _, err := tx.Exec(q, args...); err != nil {
				if err := tx.Rollback(); err != nil {
					return fmt.Errorf("exec rollback: %v", err)
				}
				return fmt.Errorf("exec transaction: %v", err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %v", err)
		}

	}
	return nil
}

type BatchOption interface {
	applyOption(*BatchOptions) error
}

type BatchOptions struct {
	BatchType gocql.BatchType
}

func (in *BatchOptions) applyOption(bo *BatchOptions) error {
	bo.BatchType = in.BatchType
	return nil
}

type Scanner interface {
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
	// TODO: check if close is needed?
	// Close() error
}

func (o *DB) Query(stmt string) (Scanner, error) {
	if o.cql != nil {
		query := o.cql.Session.Query(stmt)
		defer query.Release()
		return query.Iter().Scanner(), query.Exec()
	}
	if o.sql != nil {
		query, err := o.sql.DB.Query(stmt)
		if err != nil {
			return nil, fmt.Errorf("sql query: %v", err)
		}
		return query, nil
	}

	return nil, errors.New("no source configured")
}

type IterWithErr struct {
	iterx *gocqlx.Iterx
	err   error
}

func (iter *IterWithErr) Err() error {
	if err := iter.iterx.Close(); err != nil {
		return err
	}
	return iter.err
}

func (iter *IterWithErr) Next() bool {
	return iter.err == nil
}

func (iter *IterWithErr) Scan(dest ...interface{}) error {
	if err := iter.iterx.Get(dest[0]); err != nil {
		iter.err = err
		return iter.err
	}

	return nil
}

func (o *DB) Queryx(stmt string, names []string, args ...interface{}) (Scanner, error) {
	if o.cql != nil {
		query := o.cql.Query(stmt, names).Bind(args...)
		if err := query.Exec(); err != nil {
			return nil, fmt.Errorf("exec release: %v", err)
		}
		iter := query.Iter()

		return &IterWithErr{iter, nil}, nil
	}

	if o.sql != nil {
		named := ToNamedStatement(o.DBSource, stmt, names)
		o.Debugf("name: %v", named)
		query, err := o.sql.Queryx(named, args...)
		if err != nil {
			return nil, fmt.Errorf("sql queryx: %v", err)
		}
		return query, nil
	}

	return nil, errors.New("no source configured")
}

func (o *DB) ExecStmt(stmt string) error {
	if o.cql != nil {

		return o.cql.ExecStmt(stmt)
	}

	if o.sql != nil {
		_, err := o.sql.Exec(stmt)
		return err
	}
	return errors.New("no source configured")
}

func (o *DB) Exec(stmt string, names []string, args interface{}) error {
	if o.cql != nil {
		query := o.cql.Query(stmt, names).BindStruct(args)
		return query.ExecRelease()
	}
	if o.sql != nil {
		namedStmt := ToNamedStatement(o.DBSource, stmt, names)
		_, err := o.sql.NamedExec(namedStmt, args)
		return err
	}
	return errors.New("no source configured")
}

func (o *DB) ExecMap(stmt string, names []string, args map[string]interface{}) error {
	if o.cql != nil {
		query := o.cql.Query(stmt, names).BindMap(args)
		return query.ExecRelease()
	}
	if o.sql != nil {
		namedStmt := ToNamedStatement(o.DBSource, stmt, names)
		_, err := o.sql.NamedExec(namedStmt, args)
		return err
	}
	return errors.New("no source configured")
}

func (o *DB) ExecMany(stmt string, names []string, args ...interface{}) error {
	if o.cql != nil {
		query := o.cql.Query(stmt, names)
		defer query.Release()
		for _, arg := range args {
			query = query.Bind(arg)
			if err := query.Exec(); err != nil {
				return fmt.Errorf("cql: %w", err)
			}
		}
		return nil
	}
	if o.sql != nil {
		query, err := o.sql.PrepareNamed(ToNamedStatement(o.DBSource, stmt, names))
		if err != nil {
			return err
		}

		for _, arg := range args {
			_, err := query.Exec(arg)
			if err != nil {
				return fmt.Errorf("sql: %w", err)
			}
		}
		return query.Close()
	}
	return errors.New("no source configured")
}
