# go-sql

## Getting Started:
**Installing**
```
go get -u github.com/digital-dream-labs/go-sql@latest
```

**Supported CLI flags**

The cli flags are found in the `flags/` folder.
TLS is not supported yet.

**Building**

Build flags are required; `mysql,postgres`
```
go build -tag=postgres main.go
```
***Example***
```go
package main

import (
    sqlp "github.com/digital-dream-labs/go-sql"
    sqlf "github.com/digital-dream-labs/go-sql/flags"
)

type DB interface{
    DBX() (*sqlx.DB, error)
}

func NewDBFromContext(c *cli.Context) (DB, error)
	db, err := sqlp.New(
        // This is the Anant way... Very very bad way.
        // sql.Options{
        //     Host:        c.String(sqlf.DBHost),
        //     DBname:      c.String(sqlf.DBName),
        //     User:        c.String(sqlf.DBUser),
        //     Password:    c.String(sqlf.DBPass),
        //     DriverName:  c.String(sqlf.DBType),
        //     Port:        c.String(sqlf.DBPort),
        //     Migrate:     c.Bool(sqlf.Migrate),
        //     MigratePath: c.String(sqlf.MigratePath),
        // }
        sqlp.WithHost(c.String(sqlf.DBHost)),
        sqlp.WithDBName(c.String(sqlf.DBName)),
        sqlp.WithUser(c.String(sqlf.DBUser)),
        sqlp.WithPassword(c.String(sqlf.DBPass)),
        // These one options have defaults, and are not needed it you want to use the defaults
        // sqlp.WithType(c.String(sqlf.DBType)),
        // sqlp.WithPort(c.String(sqlf.DBPort)),
        // sqlp.WithMigrate(c.Bool(sqlf.Migrate)),
        // sqlp.MigratePath(c.String(sqlf.MigratePath)),
        }
    )
	if err != nil {
		return nil, fmt.Errorf("new db: %v", err)
	}
    return db, nil
```
## Notes:
------------------
### Migration
When a DBX() interface is called it will pull from a list of connections. This list of connections is create for testing purposes. It helps speed up testing by not create unnecessary db connections. So when using it from a testing perspective don't ever send "different" migrations paths because the subsequent New() calls will return an already existing connection.