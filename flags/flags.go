package flags

import "strings"

// FlagNamesToEnv converts flags to a ENV format
func FlagNamesToEnv(names ...string) []string {
	out := []string{}
	for _, name := range names {
		out = append(out, FlagNameToEnv(name))
	}
	return out
}

// FlagNameToEnv converts a flag to an ENV format
func FlagNameToEnv(name string) string {
	return strings.ReplaceAll(strings.ToUpper(name), "-", "_")
}
