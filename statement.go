package sql

import (
	"fmt"
	"regexp"
)

func ToNamedStatement(dbSource DBSource, stmt string, names []string) string {
	var r *regexp.Regexp
	switch dbSource {
	case DBSource_postgres:
		r = regexp.MustCompile(`\?|(\$\d)`)
	case DBSource_mysql:
		r = regexp.MustCompile(`\?|(\$\d)`)
	}
	var i int
	return r.ReplaceAllStringFunc(stmt, func(s string) string {
		defer func() { i++ }()
		return fmt.Sprintf(":%s", names[i])
	})
}

func FromQueryBuilder(dbSource DBSource, stmt string) string {
	if dbSource == DBSource_postgres {
		return regexp.MustCompile(`\$\d`).
			ReplaceAllString(stmt, "?")
	}
	return stmt
}
