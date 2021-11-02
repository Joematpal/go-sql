package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/cassandra"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	cqlreflectx "github.com/scylladb/go-reflectx"
	"github.com/scylladb/gocqlx/v2"
)

// TODO: Use master v4 version
// Current one is out of date and not supported
var dbs = &dbConnections{
	m: map[string]*DB{},
}

type dbConnections struct {
	sync.Mutex
	m map[string]*DB
}

func deleteDB(db string) {
	dbs.Lock()
	defer dbs.Unlock()
	delete(dbs.m, db)
}

func (dbc *dbConnections) GetSQLConnection(o *DB) error {
	dbc.Lock()
	defer dbc.Unlock()

	dbSource, err := o.getDataSource()
	if err != nil {
		return err
	}

	o.Debugf("source %s: %s", o.DBSource, dbSource)

	// Check if the connection exists
	if val, ok := dbc.m[dbSource]; ok {
		*o = *val
		return val.err
	}

	// Try to open a connection if it doesn't exist
	var d *sql.DB
	d, o.err = sql.Open(o.DBSource.String(), dbSource)

	if o.err == nil {
		// Convert sql to sqlx
		o.sql = sqlx.NewDb(d, o.DBSource.String())

		o.sql.Mapper = reflectx.NewMapperTagFunc(
			"json",
			preMapFunc(o.mapFunc),
			preMapFunc(o.tagMapFunc),
		)
	}

	// Add it to the pool so that some other service can reference it
	dbc.m[dbSource] = o

	// Run migrations
	if o.MigratePath != "" && o.Migrate {
		if err := RunMigrations(o); err != nil {
			return err
		}
	}
	return err
}

func (dbc *dbConnections) GetCQLConnection(o *DB) error {
	dbc.Lock()
	defer dbc.Unlock()

	// Check if the connection exists
	dbSource, err := o.getDataSource()
	if err != nil {
		return err
	}

	if val, ok := dbc.m[dbSource]; ok {
		*o = *val
		return val.err
	}

	// Try to open a connection if it doesn't exist

	cluster := gocql.NewCluster(o.Hosts...)

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: o.User,
		Password: o.Password,
	}
	cluster.ProtoVersion = 3

	//FIXME:  add in tls stuff for cql

	// Create keyspace on migration, it should fail if we try to connect to an unmigrated db
	if o.Migrate {
		ts, err := cluster.CreateSession()
		if err != nil {
			return err
		}

		if err := ts.Query(CreateListingsDevKeyspaceStmt(o.DBName)).Exec(); err != nil {
			return err
		}
	}

	cluster.Keyspace = o.DBName

	ts, err := cluster.CreateSession()
	if err != nil {
		return err
	}

	// Wrap session on creation, gocqlx session embeds gocql.Session pointer.
	session := gocqlx.NewSession(ts)
	session.Mapper = cqlreflectx.NewMapperTagFunc("json", preMapFunc(o.mapFunc), preMapFunc(o.tagMapFunc))
	o.cql = &session

	// Add it to the pool so that some other service can reference it
	dbc.m[dbSource] = o

	// Run migrations
	if o.MigratePath != "" && o.Migrate {
		if err := RunMigrations(o); err != nil {
			return err
		}
	}

	return err
}

func CreateListingsDevKeyspaceStmt(keyspace string) string {
	return `CREATE KEYSPACE IF NOT EXISTS ` + keyspace + ` WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : '1'}`
}

func RunMigrations(o *DB) error {
	var driver database.Driver
	var err error

	switch o.DBSource {
	case DBSource_postgres:
		driver, err = postgres.WithInstance(o.sql.DB, &postgres.Config{
			DatabaseName: o.DBName,
		})
		if err != nil {
			return fmt.Errorf("postgres instance: %v", err)
		}
	case DBSource_mysql:
		driver, err = mysql.WithInstance(o.sql.DB, &mysql.Config{
			DatabaseName: o.DBName,
		})
		if err != nil {
			return fmt.Errorf("mysql instance: %v", err)
		}
	case DBSource_cql:
		driver, err = cassandra.WithInstance(o.cql.Session, &cassandra.Config{
			KeyspaceName: o.DBName,
		})
		if err != nil {
			return fmt.Errorf("cql instance: %v", err)
		}
	default:
		return errors.New("db driver not supported")
	}

	m, err := migrate.NewWithDatabaseInstance(
		o.GetMigratePath(),
		o.DBName,
		driver,
	)
	if err != nil {
		return fmt.Errorf("migrations instance: %v", err)
	}

	if err := m.Up(); err != nil {
		if !errors.Is(err, migrate.ErrNoChange) {
			return fmt.Errorf("migrations up: %v", err)
		}
		o.Debugf("migrate up: %v", err)
	}
	o.Debugf("migrate up: success")
	return nil
}

func preMapFunc(f func(string) string) func(string) string {
	return func(s string) string {
		ss := strings.Split(s, ",")
		out := f(ss[0])

		return out
	}
}
