//go:build sqlite
// +build sqlite

package sql

import (
	_ "github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "modernc.org/sqlite"
)
