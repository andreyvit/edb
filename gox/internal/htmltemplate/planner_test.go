package htmltemplate

import (
	"math/rand"
	"testing"
)

func TestAppendEscapedMatchesUpstream(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	for i := range 10000 {
		data := make([]byte, i%257)
		_, _ = rng.Read(data)
		value := string(data)
		want := "prefix:" + htmlEscaper(value)
		for _, limit := range []int{len(want) - 1, len(want), len(want) + 1} {
			got, ok := AppendEscaped([]byte("prefix:"), value, limit)
			if ok != (len(want) <= limit) || ok && string(got) != want || len(got) > max(limit, 7) {
				t.Fatalf("input %q limit %d: %q, %v; want %q", value, limit, got, ok, want)
			}
		}
	}
}

func TestStringFiltersMatchUpstream(t *testing.T) {
	// Include optimized URL chains and unsupported JS/CSS/srcset filters. The
	// expected value runs every original variadic filter, even for plain HTML.
	for _, prefix := range []string{"", "<p title=\"", "<textarea>", "<a href=\"", "<a href=\"/path/", "<a href=\"?q=", "<a href=", "<img srcset=\"", "<script>let x=", "<script>let x='", "<style>a{color:", "<style>a{background:url("} {
		t.Run(prefix, func(t *testing.T) {
			c := Context{}
			if _, err := c.Text(prefix); err != nil {
				t.Fatal(err)
			}
			f, err := c.Action()
			if err != nil {
				t.Fatal(err)
			}
			inputs := []string{"javascript:alert(1)", "data:text/html,x", "https://example.invalid/a%20b?q=x&y=z", "mailto:a@example.invalid", "&<>'\"\x00", "</script>", "é\u2028\u2029", "%00%2g%"}
			rng := rand.New(rand.NewSource(72))
			for i := range 1000 {
				data := make([]byte, i%129)
				_, _ = rng.Read(data)
				inputs = append(inputs, string(data))
			}
			for _, s := range inputs {
				var want any = s
				for _, fn := range f.funcs {
					want = fn(want)
				}
				if got := f.ApplyString(s); got != want {
					t.Fatalf("input %q: got %q, want %q", s, got, want)
				}
			}
		})
	}
}
