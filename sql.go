package sql

import (
	"errors"
	"fmt"
	"regexp"

	_ "github.com/golang-migrate/migrate/source/file"
	"github.com/jmoiron/sqlx"
	"github.com/scylladb/gocqlx/v2"
)

type DBSource string

const (
	DBSource_postgres DBSource = "postgres"
	DBSource_mysql    DBSource = "mysql"
	DBSource_cql      DBSource = "cql"
)

func (s DBSource) String() string {
	return string(s)
}

func New(in ...Option) (*DB, error) {
	opts := &DB{
		MigratePath: "database/sql",
	}
	for _, opt := range in {
		if err := opt.applyOption(opts); err != nil {
			return nil, err
		}
	}

	return opts, opts.IsValid()
}

func ToNamedStatement(dbType DBSource, stmt string, names []string) string {
	var r *regexp.Regexp
	switch dbType {
	case DBSource_postgres:
		r = regexp.MustCompile(`\$\d`)
	case DBSource_mysql:
		r = regexp.MustCompile(`\?`)
	}
	var i int
	return r.ReplaceAllStringFunc(stmt, func(s string) string {
		defer func() { i++ }()
		return fmt.Sprintf(":%s", names[i])
	})
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

func (o *DB) Select(dest interface{}, query string, names []string, args ...interface{}) error {
	switch o.DBSource {
	case DBSource_postgres, DBSource_mysql:
		return o.sql.Select(ToNamedStatement(o.DBSource, query, names), query, args...)
	case DBSource_cql:
		return o.cql.Query(query, names).Select(dest)
	}
	return nil
}

func (o *DB) Get(dest interface{}, query string, names []string, args ...interface{}) error {
	switch o.DBSource {
	case DBSource_postgres, DBSource_mysql:
		return o.sql.Get(ToNamedStatement(o.DBSource, query, names), query, args...)
	case DBSource_cql:
		return o.cql.Query(query, names).Get(dest)
	}
	return nil
}
