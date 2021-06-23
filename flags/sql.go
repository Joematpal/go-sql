package flags

import "github.com/urfave/cli/v2"

var (
	DBType = "db-type"
	DBUser = "db-user"
	DBHost = "db-host"
	DBName = "db-name"
	DBPass = "db-pass"
	DBPort = "db-port"
	// TODO: we need to find a way to emplement TLS for the db drivers we support
	DBTLS       = "db-tls"
	DBPubCert   = "db-pub-cert"
	DBPrivCert  = "db-priv-cert"
	Migrate     = "migrate"
	MigratePath = "migrate-path"
	DBSource    = "db-source"
)

var DBFlags = []cli.Flag{
	&cli.StringFlag{
		Name:    DBType,
		Value:   "postgres",
		EnvVars: FlagNamesToEnv(DBType),
	},
	&cli.StringFlag{
		Name:    DBUser,
		EnvVars: FlagNamesToEnv(DBUser),
	},
	&cli.StringFlag{
		Name:    DBHost,
		EnvVars: FlagNamesToEnv(DBHost),
	},
	&cli.StringFlag{
		Name:    DBName,
		EnvVars: FlagNamesToEnv(DBName),
	},
	&cli.StringFlag{
		Name:    DBPass,
		EnvVars: FlagNamesToEnv(DBPass),
	},
	&cli.StringFlag{
		Name:    DBPort,
		Value:   "5432",
		EnvVars: FlagNamesToEnv(DBPort),
	},
	&cli.BoolFlag{
		Name:    Migrate,
		EnvVars: FlagNamesToEnv(Migrate),
	},
	&cli.StringFlag{
		Name:    MigratePath,
		Value:   "database/sql",
		EnvVars: FlagNamesToEnv(MigratePath),
	},
	&cli.StringFlag{
		Name:    DBSource,
		Value:   "",
		EnvVars: FlagNamesToEnv(DBSource),
	},
}
