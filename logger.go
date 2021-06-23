package sql

type debugger interface {
	Debugf(format string, args ...interface{})
}
