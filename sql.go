package sql

import (
	"fmt"
	"regexp"
	"strings"

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

	// Check if the DBSource is set because that means that the db driver/type is not set
	if opts.DBSource != "" {
		opts.DriverName = mysqlSource
		if strings.Contains(opts.DBSource, postgresSource) {
			opts.DriverName = postgresSource
		}
	}
	return opts, nil
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
