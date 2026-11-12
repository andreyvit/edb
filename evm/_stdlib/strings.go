package strings

func Clone(s string) string
func ToLower(s string) string
func ToUpper(s string) string
func ToTitle(s string) string
func TrimSpace(s string) string
func Compare(s string, sub string) int
func Count(s string, sub string) int
func Index(s string, sub string) int
func LastIndex(s string, sub string) int
func IndexAny(s string, sub string) int
func LastIndexAny(s string, sub string) int
func Contains(s string, sub string) bool
func ContainsAny(s string, sub string) bool
func EqualFold(s string, sub string) bool
func HasPrefix(s string, sub string) bool
func HasSuffix(s string, sub string) bool
func ContainsRune(s string, r rune) bool
func IndexRune(s string, r rune) int
func IndexByte(s string, b byte) int
func LastIndexByte(s string, b byte) int
func Cut(s string, sep string) (string, string, bool)
func CutPrefix(s string, sep string) (string, bool)
func CutSuffix(s string, sep string) (string, bool)
func Fields(s string) []string
func Join(elems []string, sep string) string
func Repeat(s string, count int) string
func Replace(s string, old string, new string, n int) string
func ReplaceAll(s string, old string, new string) string
func Split(s string, sep string) []string
func SplitAfter(s string, sep string) []string
func SplitN(s string, sep string, n int) []string
func SplitAfterN(s string, sep string, n int) []string
func Trim(s string, cut string) string
func TrimLeft(s string, cut string) string
func TrimRight(s string, cut string) string
func TrimPrefix(s string, cut string) string
func TrimSuffix(s string, cut string) string
