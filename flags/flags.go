package flags

import "strings"

// FlagNamesToEnv converts flags to a ENV format
func flagNamesToEnv(names ...string) []string {
	out := []string{}
	for _, name := range names {
		out = append(out, flagNameToEnv(name))
	}
	return out
}

// flagNameToEnv converts a flag to an ENV format
func flagNameToEnv(name string) string {
	return strings.ReplaceAll(strings.ToUpper(name), "-", "_")
}
