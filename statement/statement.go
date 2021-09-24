package statement

import (
	"fmt"
	"regexp"
)

func ToNamed(dbSource string, stmt string, names []string) string {
	var r *regexp.Regexp
	switch dbSource {
	case "postgres":
		r = regexp.MustCompile(`\$\d`)
	case "mysql", "cql":
		r = regexp.MustCompile(`\?`)
	}
	var i int
	return r.ReplaceAllStringFunc(stmt, func(s string) string {
		defer func() { i++ }()
		return fmt.Sprintf(":%s", names[i])
	})
}

func FromQueryBuilder(dbSource string, stmt string) string {
	if dbSource == "postgres" {
		return regexp.MustCompile(`\$\d`).
			ReplaceAllString(stmt, "?")
	}
	return stmt
}
