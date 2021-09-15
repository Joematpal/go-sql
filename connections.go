package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database"
	"github.com/golang-migrate/migrate/database/cassandra"
	"github.com/golang-migrate/migrate/database/mysql"
	"github.com/golang-migrate/migrate/database/postgres"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/scylladb/gocqlx/v2"
)

var dbs = &dbConnections{
	m: map[string]*Options{},
}

type dbConnections struct {
	sync.Mutex
	m map[string]*Options
}

func (dbc *dbConnections) GetSQLConnection(opts *Options) error {
	dbc.Lock()
	defer dbc.Unlock()

	dbSource, err := opts.getDataSource("")
	if err != nil {
		return err
	}

	opts.Debugf("source %s: %s", opts.DriverName, dbSource)

	// Check if the connection exists
	if val, ok := dbc.m[dbSource]; ok {
		opts = val
		return val.err
	}

	// Try to open a connection if it doesn't exist
	var d *sql.DB
	d, opts.err = sql.Open(opts.DriverName, dbSource)

	if opts.err == nil {
		// Convert sql to sqlx
		opts.sql = sqlx.NewDb(d, opts.DriverName)
		opts.sql.Mapper = reflectx.NewMapper("json")
	}

	// Add it to the pool so that some other service can reference it
	dbc.m[dbSource] = opts

	// Run migrations
	if opts.MigratePath != "" {
		var driver database.Driver

		switch opts.DriverName {
		case postgresSource:
			driver, err = postgres.WithInstance(opts.sql.DB, &postgres.Config{})
			if err != nil {
				return fmt.Errorf("postgres instance: %v", err)
			}
		case mysqlSource:
			driver, err = mysql.WithInstance(opts.sql.DB, &mysql.Config{})
			if err != nil {
				return fmt.Errorf("mysql instance: %v", err)
			}
		default:
			return errors.New("db driver not supported")
		}

		m, err := migrate.NewWithDatabaseInstance(
			opts.GetMigratePath(),
			opts.DBName,
			driver,
		)
		if err != nil {
			return fmt.Errorf("migrations instance: %v", err)
		}

		if err := m.Up(); err != nil {
			if !errors.Is(err, migrate.ErrNoChange) {
				return fmt.Errorf("migrations up: %v", err)
			}
			opts.Debugf("migrate up: %v", err)
		}
		opts.Debugf("migrate up: success")
	}

	return err
}

func (dbc *dbConnections) GetCQLConnection(opts *Options) error {
	dbc.Lock()
	defer dbc.Unlock()

	dbSource, err := opts.getDataSource("")
	if err != nil {
		return err
	}

	opts.Debugf("source %s: %s", opts.DriverName, dbSource)

	// Check if the connection exists
	if val, ok := dbc.m[dbSource]; ok {
		opts = val
		return val.err
	}

	// Try to open a connection if it doesn't exist

	cluster := gocql.NewCluster(opts.Hosts...)

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: opts.User,
		Password: opts.Password,
	}

	cluster.Keyspace = opts.DBName

	//FIXME:  add in tls stuff for cql

	// Wrap session on creation, gocqlx session embeds gocql.Session pointer.
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		return err
	}
	opts.cql = &session

	// Add it to the pool so that some other service can reference it
	dbc.m[dbSource] = opts

	// Run migrations
	if opts.MigratePath != "" {
		var driver database.Driver

		switch opts.DriverName {
		case postgresSource:
			driver, err = postgres.WithInstance(opts.sql.DB, &postgres.Config{})
			if err != nil {
				return fmt.Errorf("postgres instance: %v", err)
			}
		case mysqlSource:
			driver, err = mysql.WithInstance(opts.sql.DB, &mysql.Config{})
			if err != nil {
				return fmt.Errorf("mysql instance: %v", err)
			}
		case cqlSource:
			driver, err = cassandra.WithInstance(opts.cql.Session, &cassandra.Config{})
			if err != nil {
				return fmt.Errorf("cql instance: %v", err)
			}
		default:
			return errors.New("db driver not supported")
		}

		m, err := migrate.NewWithDatabaseInstance(
			opts.GetMigratePath(),
			opts.DBName,
			driver,
		)
		if err != nil {
			return fmt.Errorf("migrations instance: %v", err)
		}

		if err := m.Up(); err != nil {
			if !errors.Is(err, migrate.ErrNoChange) {
				return fmt.Errorf("migrations up: %v", err)
			}
			opts.Debugf("migrate up: %v", err)
		}
		opts.Debugf("migrate up: success")
	}

	return err
}
