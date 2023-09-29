package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/gocql/gocql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/cassandra"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
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

	var db *sqlx.DB
	// Try to open a connection if it doesn't exist
	db, o.err = sqlx.Open(o.DBSource.String(), dbSource)

	if o.err == nil {
		// Convert sql to sqlx
		o.sql = db

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

// CQL connection currently does not support query string arguments
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

	cluster := gocql.NewCluster(o.Hosts...)

	if o.Timeout != 0 {
		cluster.Timeout = o.Timeout
	}
	o.Debugf("cql timeout %s", cluster.Timeout)

	if o.ConnectTimeout != 0 {
		cluster.ConnectTimeout = o.ConnectTimeout
	}
	o.Debugf("cql connection timeout %s", cluster.ConnectTimeout)

	cluster.Port, err = strconv.Atoi(o.Port)
	if err != nil {
		return fmt.Errorf("atoi: %w", err)
	}

	if o.DisableInitialHostLookup {
		o.Debugf("disable initial host lookup")
		cluster.DisableInitialHostLookup = true
	}

	// Authentication
	if o.Authenticator != nil {
		cluster.Authenticator = o.Authenticator
	} else {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: o.User,
			Password: o.Password,
		}
	}

	cluster.ProtoVersion = 3

	// SSL
	if o.CaPath != "" {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath: o.CaPath,
		}
	}

	// Consistency
	cluster.Consistency = o.Consistency

	// Create keyspace on migration, it should fail if we try to connect to an unmigrated db
	if o.Migrate && o.AppEnv == development {
		o.Debugf("creating keyspace name")
		ts, err := cluster.CreateSession()
		if err != nil {
			return fmt.Errorf("create session: %v", err)
		}

		if err := ts.Query(CreateListingsDevKeyspaceStmt(o.DBName)).Exec(); err != nil {
			return err
		}
	}

	cluster.Keyspace = o.DBName

	ts, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create session: %v", err)
	}

	// Wrap session on creation, gocqlx session embeds gocql.Session pointer.
	session := gocqlx.NewSession(ts)
	session.Mapper = cqlreflectx.NewMapperTagFunc("json", preMapFunc(o.mapFunc), preMapFunc(o.tagMapFunc))
	o.cql = &session

	// Add it to the pool so that some other service can reference it
	dbc.m[dbSource] = o

	// Run migrations
	if o.MigratePath != "" && o.Migrate {
		o.Debugf("running migrations")
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
	case DBSource_sqlite:
		driver, err = sqlite.WithInstance(o.sql.DB, &sqlite.Config{
			DatabaseName: o.DBName,
		})
		if err != nil {
			return fmt.Errorf("sqlite instance: %w", err)
		}
	case DBSource_postgres:
		driver, err = postgres.WithInstance(o.sql.DB, &postgres.Config{
			DatabaseName: o.DBName,
		})
		if err != nil {
			return fmt.Errorf("postgres instance: %w", err)
		}
	case DBSource_mysql:
		driver, err = mysql.WithInstance(o.sql.DB, &mysql.Config{
			DatabaseName: o.DBName,
		})
		if err != nil {
			return fmt.Errorf("mysql instance: %w", err)
		}
	case DBSource_cql:
		driver, err = cassandra.WithInstance(o.cql.Session, &cassandra.Config{
			// CQL connection currently does not support query string arguments
			// Manually override the multi statments flag
			MultiStatementEnabled: true,
			KeyspaceName:          o.DBName,
		})
		if err != nil {
			return fmt.Errorf("cql instance: %w", err)
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
