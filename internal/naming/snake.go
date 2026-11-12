// Package naming defines identifier spelling rules for EDB frontends.
package naming

import (
	"strings"
	"unicode"
)

// Snake converts a Go identifier to snake_case. A run of capitals is one word;
// its last capital starts the next word when followed by lowercase letters
// (HTTPServer -> http_server). A trailing lowercase s pluralizes the run instead
// (AppIDs -> app_ids, URLsEnabled -> urls_enabled).
func Snake(name string) string {
	rs := []rune(name)
	var b strings.Builder
	b.Grow(len(name))
	for i, r := range rs {
		if unicode.IsUpper(r) && i > 0 {
			prev := rs[i-1]
			newWord := unicode.IsLower(prev) || unicode.IsDigit(prev)
			if unicode.IsUpper(prev) && i+1 < len(rs) && unicode.IsLower(rs[i+1]) {
				plural := rs[i+1] == 's' && (i+2 == len(rs) || !unicode.IsLower(rs[i+2]))
				newWord = !plural
			}
			if newWord {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}
