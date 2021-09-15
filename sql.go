package sql

import (
	"errors"
	"fmt"
	"regexp"

	_ "github.com/golang-migrate/migrate/source/file"
	"github.com/jmoiron/sqlx"
	"github.com/scylladb/gocqlx/v2"
)

const (
	postgresSource = "postgres"
	mysqlSource    = "mysql"
	cqlSource      = "cql"
)

func New(in ...Option) (*Options, error) {
	opts := &Options{
		MigratePath: "database/sql",
	}
	for _, opt := range in {
		if err := opt.applyOption(opts); err != nil {
			return nil, err
		}
	}

	return opts, opts.IsValid()
}

func ToNamedStatement(dbType, stmt string, names []string) string {
	var r *regexp.Regexp
	switch dbType {
	case postgresSource:
		r = regexp.MustCompile(`\$\d`)
	case mysqlSource:
		r = regexp.MustCompile(`\?`)
	}
	var i int
	return r.ReplaceAllStringFunc(stmt, func(s string) string {
		defer func() { i++ }()
		return fmt.Sprintf(":%s", names[i])
	})
}
func (o *Options) SQLX() (*sqlx.DB, error) {
	switch o.DBSource {
	case mysqlSource, postgresSource:
		return o.sql, nil
	}

	return nil, errors.New("sql is not currently configured")
}

func (o *Options) CQLX() (*gocqlx.Session, error) {
	if o.DBSource != cqlSource {
		return nil, errors.New("cql is not currently configured")
	}

	return o.cql, nil
}

func (o *Options) Select(dest interface{}, query string, names []string, args ...interface{}) error {
	switch o.DBSource {
	case postgresSource, mysqlSource:
		return o.sql.Select(ToNamedStatement(o.DBName, query, names), query, args...)
	case cqlSource:
		return o.cql.Query(query, names).Select(dest)
	}
	return nil
}

func (o *Options) Get(dest interface{}, query string, names []string, args ...interface{}) error {
	switch o.DBSource {
	case postgresSource, mysqlSource:
		return o.sql.Get(ToNamedStatement(o.DBName, query, names), query, args...)
	case cqlSource:
		return o.cql.Query(query, names).Get(dest)
	}
	return nil
}
