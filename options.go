package sql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Options struct {
	DriverName  string
	DB          *sqlx.DB
	User        string
	Host        string
	DBname      string
	Password    string
	Port        string
	Migrate     bool
	MigratePath string
}

func (opts *Options) applyOption(out *Options) error {
	if opts.DriverName != "" {
		out.DriverName = opts.DriverName
	}
	if opts.DB != nil {
		out.DB = opts.DB
	}
	if opts.User != "" {
		out.User = opts.User
	}
	if opts.Host != "" {
		out.Host = opts.Host
	}
	if opts.DBname != "" {
		out.DBname = opts.DBname
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
	return nil
}

// GetMigratePath will add the protocol if it is not there assuming that the path is a local file
func (opts *Options) GetMigratePath() string {
	if strings.Contains(opts.MigratePath, "://") {
		return opts.MigratePath
	}
	return fmt.Sprintf("file://%s", opts.MigratePath)
}

func (opts *Options) DBX() (*sqlx.DB, error) {
	if opts.DriverName == "" {
		return nil, errors.New("please provide a driver type")
	}

	if opts.DB != nil {
		return opts.DB, nil
	}

	// TODO: add tls support for the data source
	connection, err := opts.getDataSource("")
	if err != nil {
		return nil, err
	}

	dbOpts := []ConnectOption{}

	if opts.Migrate {
		mpath := opts.GetMigratePath()
		fmt.Println("mpath", mpath)
		dbOpts = append(dbOpts, withMigratePath(mpath))
	}

	db, err := dbs.DBX(opts.DriverName, connection, dbOpts...)
	if err != nil {
		return nil, fmt.Errorf("get sqlx from : %v", err)
	}

	return db, nil
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
		opts.Host = host
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
		opts.DBname = name
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

func (opts *Options) getDataSource(custom string) (string, error) {
	switch opts.DriverName {
	case mysqlSource:
		return opts.getMysqlDataSource(custom)
	case postgresSource:
		return opts.getPostgresDataSource(custom)
	default:
		return "", errors.New("only mysql and postgres are supported")
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
	if opts.Host == "" {
		return sb.String(), errors.New("db host cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf("tcp(%s", opts.Host)); err != nil {
		return sb.String(), err
	}
	if opts.Port == "" {
		return sb.String(), errors.New("db port cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s)", opts.Port)); err != nil {
		return sb.String(), err
	}
	if opts.DBname == "" {
		return sb.String(), errors.New("db dbname cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf("/%s", opts.DBname)); err != nil {
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
	if opts.Host == "" {
		return sb.String(), errors.New("db host cannot be an empty string")
	}
	if _, err := sb.WriteString(opts.Host); err != nil {
		return sb.String(), err
	}
	if opts.Port == "" {
		return sb.String(), errors.New("db port cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf(":%s", opts.Port)); err != nil {
		return sb.String(), err
	}
	if opts.DBname == "" {
		return sb.String(), errors.New("db dbname cannot be an empty string")
	}
	if _, err := sb.WriteString(fmt.Sprintf("/%s", opts.DBname)); err != nil {
		return sb.String(), err
	}

	// TODO: Add in the TLS support
	if _, err := sb.WriteString(fmt.Sprintf("?sslmode=%s", "disable")); err != nil {
		return sb.String(), err
	}
	return sb.String(), nil
}
