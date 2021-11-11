package flags

import "github.com/urfave/cli/v2"

const (
	AWSAccessKeyID     = "aws-access-key-id"
	AWSSecretAccessKey = "aws-secret-access-key"
	AWSSessionToken    = "aws-session-token"
)

var AWSCQLAuthFlags = []cli.Flag{
	&cli.StringFlag{
		Name:    AWSAccessKeyID,
		EnvVars: flagNamesToEnv(AWSAccessKeyID),
	},
	&cli.StringFlag{
		Name:    AWSSecretAccessKey,
		EnvVars: flagNamesToEnv(AWSSecretAccessKey),
	},
	&cli.StringFlag{
		Name:    AWSSessionToken,
		EnvVars: flagNamesToEnv(AWSSessionToken),
	},
}
