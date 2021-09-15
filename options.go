package sql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/scylladb/gocqlx/v2"
)

type DB struct {
	sql         *sqlx.DB
	User        string
	Hosts       []string
	DBName      string
	Password    string
	Port        string
	Migrate     bool
	MigratePath string
	DBSource    DBSource
	Debugger    Debugger
	err         error
	cql         *gocqlx.Session
}

func (opts *DB) applyOption(out *DB) error {

	if opts.sql != nil {
		out.sql = opts.sql
	}
	if opts.User != "" {
		out.User = opts.User
	}
	if len(opts.Hosts) != 0 {
		out.Hosts = append(out.Hosts, opts.Hosts...)
	}
	if opts.DBName != "" {
		out.DBName = opts.DBName
	}
	if opts.Password != "" {
		out.Password = opts.Password
	}
	if opts.Port != "" {
		out.Port = opts.Port
	}

	out.Migrate = opts.Migrate

	if opts.MigratePath != "" {
		out.MigratePath = opts.MigratePath
	}

	if opts.Debugger != nil {
		out.Debugger = opts.Debugger
	}

	if opts.DBSource == "" {
		out.DBSource = opts.DBSource
	}

	return nil
}

func (opts *DB) IsValid() error {
	// Check if the DBSource is set because that means that the db driver/type is not set

	switch opts.DBSource {
	case DBSource_postgres, DBSource_mysql:
		// Check if there is already and existing connection
		if err := dbs.GetSQLConnection(opts); err != nil {
			return fmt.Errorf("sql conn: %v", err)
		}
	case DBSource_cql:
		// Check if there is already and existing connection
		if err := dbs.GetCQLConnection(opts); err != nil {
			return fmt.Errorf("cql conn: %v", err)
		}
	}

	return nil
}

// GetMigratePath will add the protocol if it is not there assuming that the path is a local file
func (opts *DB) GetMigratePath() string {
	if strings.Contains(opts.MigratePath, "://") {
		return opts.MigratePath
	}
	return fmt.Sprintf("file://%s", opts.MigratePath)
}

func (opts *DB) Debugf(format string, args ...interface{}) {
	if opts.Debugger != nil {
		opts.Debugger.Debugf(format, args...)
	}
}

// Option interface to add values to the opts struct
type Option interface {
	applyOption(*DB) error
}

type optionApplyFunc func(*DB) error

func (f optionApplyFunc) applyOption(opts *DB) error {
	return f(opts)
}

// WithHost pass in the ip or fqdn of where the db is hosted
func WithHost(host string) Option {
	return optionApplyFunc(func(opts *DB) error {
		opts.Hosts = append(opts.Hosts, host)
		return nil
	})
}

func WithHosts(hosts ...string) Option {
	return optionApplyFunc(func(opts *DB) error {
		opts.Hosts = append(opts.Hosts, hosts...)
		return nil
	})
}

// WithType pass in the type of sql db being used
func WithType(driverName string) Option {
	return WithDBSource(driverName)
}

// WithUser pass in the user
func WithUser(user string) Option {
	return optionApplyFunc(func(opts *DB) error {
		opts.User = user
		return nil
	})
}

// WithPassword pass in the password
func WithPassword(password string) Option {
	return optionApplyFunc(func(opts *DB) error {
		opts.Password = password
		return nil
	})
}

// WithDBName pass in the name of the db
func WithDBName(name string) Option {
	return optionApplyFunc(func(opts *DB) error {
		opts.DBName = name
		return nil
	})
}

// WithPort pass in the port the db is hosted on
func WithPort(port string) Option {
	return optionApplyFunc(func(opts *DB) error {
		opts.Port = port
		return nil
	})
}

// WithMigrate pass in the flag if the db is to apply migrations
func WithMigrate(migrate bool) Option {
	return optionApplyFunc(func(opts *DB) error {
		opts.Migrate = migrate
		return nil
	})
}

// WithMigratePath pass in the filepath the db needs for the migrations that need to be added
func WithMigratePath(migratePath string) Option {
	return optionApplyFunc(func(opts *DB) error {
		opts.MigratePath = migratePath
		return nil
	})
}

// WithDBSource ...
func WithDBSource(dbSource string) Option {
	return optionApplyFunc(func(opts *DB) error {
		switch DBSource(dbSource) {
		case DBSource_postgres:
			opts.DBSource = DBSource_postgres
		case DBSource_mysql:
			opts.DBSource = DBSource_mysql
		case DBSource_cql:
			opts.DBSource = DBSource_cql
		default:
			return fmt.Errorf("dbsource %s not supported", dbSource)
		}
		return nil
	})
}

// WithDebugger pass in the debugger the db can use for debug statements
func WithDebugger(debugger Debugger) Option {
	return optionApplyFunc(func(opts *DB) error {
		opts.Debugger = debugger
		return nil
	})
}

func (opts *DB) getDataSource(custom string) (string, error) {

	switch opts.DBSource {
	case DBSource_mysql:
		return opts.getMysqlDataSource(custom)
	case DBSource_postgres:
		return opts.getPostgresDataSource(custom)
	case DBSource_cql:
		return "", nil
	default:
		return "", errors.New("only mysql, postgres and cql are supported")
	}
}

// mysql connection string
// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
func (opts *DB) getMysqlDataSource(custom string) (string, error) {
	var sb strings.Builder
	if opts.User == "" {
		return sb.String(), errors.New("db user cannot be an empty string")
	}
	if _, err := sb.WriteString(opts.User); err != nil {
		return sb.String(), err
	}
	if opts.Password == "" {
		return sb.String(), errors.New("db password cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s@", opts.Password)); err != nil {
		return sb.String(), err
	}
	if len(opts.Hosts) == 0 {
		return sb.String(), errors.New("db host cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf("tcp(%s", opts.Hosts[0])); err != nil {
		return sb.String(), err
	}
	if opts.Port == "" {
		return sb.String(), errors.New("db port cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s)", opts.Port)); err != nil {
		return sb.String(), err
	}
	if opts.DBName == "" {
		return sb.String(), errors.New("db dbname cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf("/%s", opts.DBName)); err != nil {
		return sb.String(), err
	}
	// TODO: Add in the TLS support
	return sb.String(), nil
}

// postgres connection string
// "postgres://[username[:password]]@[host]/dbname?sslmode=verify-full"
func (opts *DB) getPostgresDataSource(custom string) (string, error) {
	var sb strings.Builder
	sb.WriteString("postgres://")
	if opts.User == "" {
		return sb.String(), errors.New("db user cannot be an empty string")
	}
	if _, err := sb.WriteString(opts.User); err != nil {
		return sb.String(), err
	}
	if opts.Password == "" {
		return sb.String(), errors.New("db password cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s@", opts.Password)); err != nil {
		return sb.String(), err
	}
	if len(opts.Hosts) == 0 {
		return sb.String(), errors.New("db host cannot be an empty string")
	}
	if _, err := sb.WriteString(opts.Hosts[0]); err != nil {
		return sb.String(), err
	}
	if opts.Port == "" {
		return sb.String(), errors.New("db port cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s", opts.Port)); err != nil {
		return sb.String(), err
	}
	if opts.DBName == "" {
		return sb.String(), errors.New("db dbname cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf("/%s", opts.DBName)); err != nil {
		return sb.String(), err
	}

	// TODO: Add in the TLS support
	if _, err := sb.WriteString(fmt.Sprintf("?sslmode=%s", "disable")); err != nil {
		return sb.String(), err
	}
	return sb.String(), nil
}
