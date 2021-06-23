package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database"
	"github.com/golang-migrate/migrate/database/mysql"
	"github.com/golang-migrate/migrate/database/postgres"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
)

type dbConnection struct {
	db       *sqlx.DB
	debugger debugger
	err      error
}

func (dbc *dbConnection) Debugf(format string, args ...interface{}) {
	if dbc.debugger != nil {
		dbc.debugger.Debugf(format, args)
	}
}

type dbConnections struct {
	sync.Mutex
	m map[string]*dbConnection
}

type connectionOptions struct {
	migratePath string
	debugger    debugger
}

type ConnectOption interface {
	applyOption(*connectionOptions) error
}

type connectOptionApplyFunc func(*connectionOptions) error

func (f connectOptionApplyFunc) applyOption(opts *connectionOptions) error {
	return f(opts)
}

func (dbc *dbConnections) DBX(driverName string, connection string, opts ...ConnectOption) (*sqlx.DB, error) {
	dbc.Lock()
	defer dbc.Unlock()

	cOpts := &connectionOptions{}
	for _, opt := range opts {
		if err := opt.applyOption(cOpts); err != nil {
			return nil, err
		}
	}
	// Check if the connection exists
	if val, ok := dbc.m[connection]; ok {
		return val.db, val.err
	}

	// Try to open a connection if it doesn't exist
	d, err := sql.Open(driverName, connection)

	// Convert sql to sqlx
	db := sqlx.NewDb(d, driverName)
	db.Mapper = reflectx.NewMapper("json")

	// Add it to the pool so that some other service can reference it
	dbConn := &dbConnection{
		db:       db,
		debugger: cOpts.debugger,
		err:      err,
	}
	dbc.m[connection] = dbConn

	// Run migrations
	if cOpts.migratePath != "" {
		var driver database.Driver

		switch driverName {
		case postgresSource:
			driver, err = postgres.WithInstance(db.DB, &postgres.Config{})
			if err != nil {
				return nil, fmt.Errorf("postgres instance: %v", err)
			}
		case mysqlSource:
			driver, err = mysql.WithInstance(db.DB, &mysql.Config{})
			if err != nil {
				return nil, fmt.Errorf("mysql instance: %v", err)
			}
		default:
			return nil, errors.New("db driver not supported")
		}

		m, err := migrate.NewWithDatabaseInstance(
			cOpts.migratePath,
			driverName,
			driver,
		)
		if err != nil {
			return nil, fmt.Errorf("migrations instance: %v", err)
		}

		if err := m.Up(); err != nil {
			if !errors.Is(err, migrate.ErrNoChange) {
				return nil, fmt.Errorf("migrations up: %v", err)
			}
			dbConn.Debugf("migrate up: %v", err)
		}
		dbConn.Debugf("migrate up: success")
	}

	return db, err
}

var dbs = &dbConnections{
	m: map[string]*dbConnection{},
}

func withMigratePath(migratePath string) ConnectOption {
	return connectOptionApplyFunc(func(co *connectionOptions) error {
		if migratePath == "" {
			return errors.New("migrate path cannot be empty")
		}
		co.migratePath = migratePath
		return nil
	})
}

func withDebugger(debugger debugger) ConnectOption {
	return connectOptionApplyFunc(func(co *connectionOptions) error {
		if debugger != nil {
			co.debugger = debugger
		}
		return nil
	})
}
