// +build postgres

package sql

import (
	_ "github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/lib/pq"
)
