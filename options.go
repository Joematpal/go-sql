package sql

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

func (o *DB) applyOption(out *DB) error {

	if o.sql != nil {
		out.sql = o.sql
	}
	if o.User != "" {
		out.User = o.User
	}
	if len(o.Hosts) != 0 {
		out.Hosts = append(out.Hosts, o.Hosts...)
	}
	if o.DBName != "" {
		out.DBName = o.DBName
	}
	if o.Password != "" {
		out.Password = o.Password
	}
	if o.Port != "" {
		out.Port = o.Port
	}

	out.Migrate = o.Migrate

	if o.MigratePath != "" {
		out.MigratePath = o.MigratePath
	}

	if o.Debugger != nil {
		out.Debugger = o.Debugger
	}

	if o.DBSource != "" {
		out.DBSource = o.DBSource
	}

	if o.Authenticator != nil {
		out.Authenticator = o.Authenticator
	}

	out.DisableInitialHostLookup = o.DisableInitialHostLookup

	if out.Consistency != 0 {
		out.Consistency = o.Consistency
	}

	if o.Timeout != 0 {
		out.Timeout = o.Timeout
	}

	if o.ConnectTimeout != 0 {
		out.ConnectTimeout = o.ConnectTimeout
	}

	return nil
}

func (o *DB) IsValid() error {
	// Check if the DBSource is set because that means that the db driver/type is not set
	switch o.DBSource {
	case DBSource_postgres, DBSource_mysql, DBSource_sqlite:
		// Check if there is already and existing connection
		if err := dbs.GetSQLConnection(o); err != nil {
			return fmt.Errorf("sql conn: %v", err)
		}
		return nil
	case DBSource_cql:
		// Check if there is already and existing connection
		if err := dbs.GetCQLConnection(o); err != nil {
			return fmt.Errorf("cql conn: %v", err)
		}
		return nil
	}

	return fmt.Errorf("db source %s is not supported", o.DBSource)
}

// GetMigratePath will add the protocol if it is not there assuming that the path is a local file
func (o *DB) GetMigratePath() string {
	if strings.Contains(o.MigratePath, "://") {
		return o.MigratePath
	}
	return fmt.Sprintf("file://%s", o.MigratePath)
}

func (o *DB) Debugf(format string, args ...interface{}) {
	if o.Debugger != nil {
		o.Debugger.Debugf(format, args...)
	}
}

// Option interface to add values to the o struct
type Option interface {
	applyOption(*DB) error
}

type optionApplyFunc func(*DB) error

func (f optionApplyFunc) applyOption(o *DB) error {
	return f(o)
}

// WithHost pass in the ip or fqdn of where the db is hosted
func WithHost(host string) Option {
	return optionApplyFunc(func(o *DB) error {
		o.Hosts = append(o.Hosts, host)
		return nil
	})
}

func WithHosts(hosts ...string) Option {
	return optionApplyFunc(func(o *DB) error {
		o.Hosts = append(o.Hosts, hosts...)
		return nil
	})
}

// WithType pass in the type of sql db being used
func WithType(driverName string) Option {
	return WithDBSource(driverName)
}

// WithUser pass in the user
func WithUser(user string) Option {
	return optionApplyFunc(func(o *DB) error {
		o.User = user
		return nil
	})
}

// WithPassword pass in the password
func WithPassword(password string) Option {
	return optionApplyFunc(func(o *DB) error {
		o.Password = password
		return nil
	})
}

// WithDBName pass in the name of the db
func WithDBName(name string) Option {
	return optionApplyFunc(func(o *DB) error {
		o.DBName = name
		return nil
	})
}

// WithPort pass in the port the db is hosted on
func WithPort(port string) Option {
	return optionApplyFunc(func(o *DB) error {
		o.Port = port
		return nil
	})
}

// WithMigrate pass in the flag if the db is to apply migrations
func WithMigrate(migrate bool) Option {
	return optionApplyFunc(func(o *DB) error {
		o.Migrate = migrate
		return nil
	})
}

// WithMigratePath pass in the filepath the db needs for the migrations that need to be added
func WithMigratePath(migratePath string) Option {
	return optionApplyFunc(func(o *DB) error {
		o.MigratePath = migratePath
		return nil
	})
}

func WithAppEnv(appEnv string) Option {
	return optionApplyFunc(func(d *DB) error {
		switch appEnv {
		case production, development:
			d.AppEnv = appEnv
			return nil
		default:
			return fmt.Errorf("only %s and %s are supported", production, development)
		}
	})
}

// WithDBSource ...
func WithDBSource(dbSource string) Option {
	return optionApplyFunc(func(o *DB) error {
		switch DBSource(dbSource) {
		case DBSource_postgres:
			o.DBSource = DBSource_postgres
		case DBSource_mysql:
			o.DBSource = DBSource_mysql
		case DBSource_cql:
			o.DBSource = DBSource_cql
		default:
			return fmt.Errorf("dbsource %s not supported", dbSource)
		}
		return nil
	})
}

// WithDebugger pass in the debugger the db can use for debug statements
func WithDebugger(debugger Debugger) Option {
	return optionApplyFunc(func(o *DB) error {
		o.Debugger = debugger
		return nil
	})
}

func WithDatabaseConnectionString(s string) Option {
	return optionApplyFunc(func(d *DB) error {
		// Parse the db connection string
		db, err := parseDBConnectionString(s)
		if err != nil {
			return err
		}

		if err := db.applyOption(d); err != nil {
			return err
		}

		return nil
	})
}

func WithAuthenticator(authenticator gocql.Authenticator) Option {
	return optionApplyFunc(func(d *DB) error {
		d.Authenticator = authenticator
		return nil
	})
}

func WithConsistency(consistency gocql.Consistency) Option {
	return optionApplyFunc(func(d *DB) error {
		d.Consistency = consistency
		return nil
	})
}

func WithDisableInitialHostLookup() Option {
	return optionApplyFunc(func(d *DB) error {
		d.DisableInitialHostLookup = true
		return nil
	})
}

func WithCertificateAuthority(path string) Option {
	return optionApplyFunc(func(d *DB) error {
		d.CaPath = path
		return nil
	})
}

func WithTimeout(timeout time.Duration) Option {
	return optionApplyFunc(func(d *DB) error {
		d.Timeout = timeout
		return nil
	})
}

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return optionApplyFunc(func(d *DB) error {
		d.ConnectTimeout = connectTimeout
		return nil
	})
}

func WithRawQuery(rawQuery string) Option {
	return optionApplyFunc(func(d *DB) error {
		d.RawQuery = rawQuery
		return nil
	})
}

// CQL connection currently does not support query string arguments
func (o *DB) getDataSource() (string, error) {

	switch o.DBSource {
	case DBSource_mysql:
		return o.getMysqlDataSource()
	case DBSource_postgres:
		return o.getPostgresDataSource()
	case DBSource_sqlite:
		return o.GetMigratePath(), nil
	case DBSource_cql:
		b, err := json.Marshal(o)
		return string(b), err
	default:
		return "", errors.New("only mysql, postgres, sqlite and cql are supported")
	}
}

func WithMapFunc(mapFunc func(string) string) Option {
	return optionApplyFunc(func(d *DB) error {
		d.mapFunc = mapFunc
		return nil
	})
}

func WithTagMapFunc(tagMapFunc func(string) string) Option {
	return optionApplyFunc(func(d *DB) error {
		d.tagMapFunc = tagMapFunc
		return nil
	})
}

// mysql connection string
// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
func (o *DB) getMysqlDataSource() (string, error) {
	var sb strings.Builder
	if o.User == "" {
		return sb.String(), errors.New("db user cannot be an empty string")
	}
	if _, err := sb.WriteString(o.User); err != nil {
		return sb.String(), err
	}
	if o.Password == "" {
		return sb.String(), errors.New("db password cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s@", o.Password)); err != nil {
		return sb.String(), err
	}
	if len(o.Hosts) == 0 {
		return sb.String(), errors.New("db host cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf("tcp(%s", o.Hosts[0])); err != nil {
		return sb.String(), err
	}
	if o.Port == "" {
		return sb.String(), errors.New("db port cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s)", o.Port)); err != nil {
		return sb.String(), err
	}
	if o.DBName == "" {
		return sb.String(), errors.New("db dbname cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf("/%s?", o.DBName)); err != nil {
		return sb.String(), err
	}

	if _, err := sb.WriteString(o.RawQuery); err != nil {
		return sb.String(), err
	}

	if !strings.Contains(o.RawQuery, "multiStatements") {
		if _, err := sb.WriteString("&multiStatements=true"); err != nil {
			return sb.String(), err
		}
	}

	// TODO: Add in the TLS support
	return sb.String(), nil
}

// postgres connection string
// "postgres://[username[:password]]@[host]/dbname?sslmode=verify-full"
func (o *DB) getPostgresDataSource() (string, error) {
	var sb strings.Builder
	sb.WriteString("postgres://")
	if o.User == "" {
		return sb.String(), errors.New("db user cannot be an empty string")
	}
	if _, err := sb.WriteString(o.User); err != nil {
		return sb.String(), err
	}
	if o.Password == "" {
		return sb.String(), errors.New("db password cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s@", o.Password)); err != nil {
		return sb.String(), err
	}
	if len(o.Hosts) == 0 {
		return sb.String(), errors.New("db host cannot be an empty string")
	}
	if _, err := sb.WriteString(o.Hosts[0]); err != nil {
		return sb.String(), err
	}
	if o.Port == "" {
		return sb.String(), errors.New("db port cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s", o.Port)); err != nil {
		return sb.String(), err
	}
	if o.DBName == "" {
		return sb.String(), errors.New("db dbname cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf("/%s?", o.DBName)); err != nil {
		return sb.String(), err
	}

	// TODO: Add in the TLS support
	if !strings.Contains(o.RawQuery, "sslmode") {
		if _, err := sb.WriteString(fmt.Sprintf("sslmode=%s", "disable")); err != nil {
			return sb.String(), err
		}
	}

	if _, err := sb.WriteString(fmt.Sprintf("&%s", o.RawQuery)); err != nil {
		return sb.String(), err
	}

	if !strings.Contains(o.RawQuery, "x-multi-statements") {
		if _, err := sb.WriteString("&x-multi-statements=true"); err != nil {
			return sb.String(), err
		}
	}

	return sb.String(), nil
}

func parseDBConnectionString(s string) (*DB, error) {

	// Does not support mysql's tcp()
	// mysql://username:password@tcp(host:port)/dbname?query
	// postgres://username:password@host:port/dbname?query
	// postgresql://username:password@host:port/dbname?query
	db := &DB{}

	u, err := url.Parse(s)
	if err != nil {
		return nil, err
	}

	// dbsource
	switch u.Scheme {
	case "postgresql", "postgres":
		db.DBSource = DBSource_postgres
	case "cassandra":
		db.DBSource = DBSource_cql
	case "mysql":
		db.DBSource = DBSource_mysql
	default:
		return nil, errors.New("source not supported")
	}

	// copy over query info
	db.RawQuery = u.RawQuery

	// username
	db.User = u.User.Username()
	// password
	if passwd, ok := u.User.Password(); ok {
		db.Password = passwd
	}
	// host
	db.Hosts = []string{strings.TrimRight(u.Host, ":"+u.Port())}

	// port
	db.Port = u.Port()

	// dbname
	db.DBName = strings.Split(strings.TrimLeft(u.Path, "/"), "/")[0]

	return db, nil
}
