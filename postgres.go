//go:build postgres
// +build postgres

package sql

import (
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/jackc/pgx/v5"
)
