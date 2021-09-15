package sql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/scylladb/gocqlx/v2"
)

type Options struct {
	DriverName  string
	sql         *sqlx.DB
	User        string
	Hosts       []string
	DBName      string
	Password    string
	Port        string
	Migrate     bool
	MigratePath string
	DBSource    string
	Debugger    Debugger
	err         error
	cql         *gocqlx.Session
}

func (opts *Options) applyOption(out *Options) error {
	if opts.DriverName != "" {
		out.DriverName = opts.DriverName
	}
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

func (opts *Options) IsValid() error {
	// Check if the DBSource is set because that means that the db driver/type is not set

	if opts.DBSource != "" {
		opts.DriverName = mysqlSource
		if strings.Contains(opts.DBSource, postgresSource) {
			opts.DriverName = postgresSource
		}
	}

	switch opts.DBSource {
	case postgresSource, mysqlSource:
		// Check if there is already and existing connection
		if err := dbs.GetSQLConnection(opts); err != nil {
			return fmt.Errorf("sql conn: %v", err)
		}
	case cqlSource:
		// Check if there is already and existing connection
		if err := dbs.GetCQLConnection(opts); err != nil {
			return fmt.Errorf("sql conn: %v", err)
		}
	}

	return nil
}

// GetMigratePath will add the protocol if it is not there assuming that the path is a local file
func (opts *Options) GetMigratePath() string {
	if strings.Contains(opts.MigratePath, "://") {
		return opts.MigratePath
	}
	return fmt.Sprintf("file://%s", opts.MigratePath)
}

func (opts *Options) Debugf(format string, args ...interface{}) {
	if opts.Debugger != nil {
		opts.Debugger.Debugf(format, args...)
	}
}

// Option interface to add values to the opts struct
type Option interface {
	applyOption(*Options) error
}

type optionApplyFunc func(*Options) error

func (f optionApplyFunc) applyOption(opts *Options) error {
	return f(opts)
}

// WithHost pass in the ip or fqdn of where the db is hosted
func WithHost(host string) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.Hosts = append(opts.Hosts, host)
		return nil
	})
}

func WithHosts(hosts ...string) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.Hosts = append(opts.Hosts, hosts...)
		return nil
	})
}

// WithType pass in the type of sql db being used
func WithType(driverName string) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.DriverName = driverName
		return nil
	})
}

// WithUser pass in the user
func WithUser(user string) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.User = user
		return nil
	})
}

// WithPassword pass in the password
func WithPassword(password string) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.Password = password
		return nil
	})
}

// WithDBName pass in the name of the db
func WithDBName(name string) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.DBName = name
		return nil
	})
}

// WithPort pass in the port the db is hosted on
func WithPort(port string) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.Port = port
		return nil
	})
}

// WithMigrate pass in the flag if the db is to apply migrations
func WithMigrate(migrate bool) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.Migrate = migrate
		return nil
	})
}

// WithMigratePath pass in the filepath the db needs for the migrations that need to be added
func WithMigratePath(migratePath string) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.MigratePath = migratePath
		return nil
	})
}

// WithDBSource ...
func WithDBSource(dbSource string) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.DBSource = dbSource
		return nil
	})
}

// WithDebugger pass in the debugger the db can use for debug statements
func WithDebugger(debugger Debugger) Option {
	return optionApplyFunc(func(opts *Options) error {
		opts.Debugger = debugger
		return nil
	})
}

func (opts *Options) getDataSource(custom string) (string, error) {
	if opts.DBSource != "" {
		return opts.DBSource, nil
	}

	switch opts.DriverName {
	case mysqlSource:
		return opts.getMysqlDataSource(custom)
	case postgresSource:
		return opts.getPostgresDataSource(custom)
	default:
		return "", errors.New("only mysql, postgres and cql are supported")
	}
}

// mysql connection string
// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
func (opts *Options) getMysqlDataSource(custom string) (string, error) {
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
func (opts *Options) getPostgresDataSource(custom string) (string, error) {
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
