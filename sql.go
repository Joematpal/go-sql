package sql

import (
	"fmt"
	"regexp"

	_ "github.com/golang-migrate/migrate/source/file"
	"github.com/jmoiron/sqlx"
)

const (
	postgresSource = "postgres"
	mysqlSource    = "mysql"
)

type DB interface {
	DBX() (*sqlx.DB, error)
}

func New(in ...Option) (DB, error) {
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
