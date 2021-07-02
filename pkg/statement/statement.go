package statement

import (
	"fmt"
	"regexp"
)

func ToNamed(dbType, stmt string, names []string) string {
	var r *regexp.Regexp
	switch dbType {
	case "postgres":
		r = regexp.MustCompile(`\$\d`)
	case "mysql":
		r = regexp.MustCompile(`\?`)
	}
	var i int
	return r.ReplaceAllStringFunc(stmt, func(s string) string {
		defer func() { i++ }()
		return fmt.Sprintf(":%s", names[i])
	})
}
