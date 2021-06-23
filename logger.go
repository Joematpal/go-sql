package sql

type Debugger interface {
	Debugf(format string, args ...interface{})
}
