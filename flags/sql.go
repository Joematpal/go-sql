package flags

import "github.com/urfave/cli/v2"

const (
	DBType  = "db-type"
	DBUser  = "db-user"
	DBHosts = "db-hosts"
	DBName  = "db-name"
	DBPass  = "db-pass"
	DBPort  = "db-port"
	// TODO: we need to find a way to implement TLS for the db drivers we support
	DBTLS                  = "db-tls"
	DBCertificateAuthority = "db-ca-cert"
	DBPubCert              = "db-pub-cert"
	DBPrivCert             = "db-priv-cert"
	Migrate                = "migrate"
	MigratePath            = "migrate-path"
	DBSource               = "db-source"
)

var DBFlags = []cli.Flag{
	&cli.StringFlag{
		Name:    DBType,
		Value:   "postgres",
		EnvVars: flagNamesToEnv(DBType),
	},
	&cli.StringFlag{
		Name:    DBUser,
		EnvVars: flagNamesToEnv(DBUser),
	},
	&cli.StringFlag{
		Name:    DBHosts,
		EnvVars: flagNamesToEnv(DBHosts),
	},
	&cli.StringFlag{
		Name:    DBName,
		EnvVars: flagNamesToEnv(DBName),
	},
	&cli.StringFlag{
		Name:    DBPass,
		EnvVars: flagNamesToEnv(DBPass),
	},
	&cli.StringFlag{
		Name:    DBPort,
		Value:   "5432",
		EnvVars: flagNamesToEnv(DBPort),
	},
	&cli.BoolFlag{
		Name:    Migrate,
		EnvVars: flagNamesToEnv(Migrate),
	},
	&cli.StringFlag{
		Name:    MigratePath,
		Value:   "database/sql",
		EnvVars: flagNamesToEnv(MigratePath),
	},
	&cli.StringFlag{
		Name:    DBSource,
		Value:   "",
		EnvVars: flagNamesToEnv(DBSource),
	},
	&cli.BoolFlag{
		Name:    DBTLS,
		EnvVars: flagNamesToEnv(DBTLS),
	},
	&cli.StringFlag{
		Name:    DBCertificateAuthority,
		EnvVars: flagNamesToEnv((DBCertificateAuthority)),
	},
}
