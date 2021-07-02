package sql

import (
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
