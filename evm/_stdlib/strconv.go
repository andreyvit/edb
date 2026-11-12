package strconv

import "errors"

func atoi(s string) (int, string)
func Atoi(s string) (int, error) {
	v, text := atoi(s)
	if text != "" {
		return v, errors.New(text)
	}
	return v, nil
}
func Itoa(i int) string
func parseBool(str string) (bool, string)
func ParseBool(str string) (bool, error) {
	v, text := parseBool(str)
	if text != "" {
		return v, errors.New(text)
	}
	return v, nil
}
func parseInt(s string, base int, bitSize int) (int64, string)
func ParseInt(s string, base int, bitSize int) (int64, error) {
	v, text := parseInt(s, base, bitSize)
	if text != "" {
		return v, errors.New(text)
	}
	return v, nil
}
func parseUint(s string, base int, bitSize int) (uint64, string)
func ParseUint(s string, base int, bitSize int) (uint64, error) {
	v, text := parseUint(s, base, bitSize)
	if text != "" {
		return v, errors.New(text)
	}
	return v, nil
}
func parseFloat(s string, bitSize int) (float64, string)
func ParseFloat(s string, bitSize int) (float64, error) {
	v, text := parseFloat(s, bitSize)
	if text != "" {
		return v, errors.New(text)
	}
	return v, nil
}
func FormatBool(b bool) string
func FormatInt(i int64, base int) string
func FormatUint(i uint64, base int) string
func FormatFloat(f float64, fmt byte, prec int, bitSize int) string
func Quote(s string) string
func QuoteToASCII(s string) string
func unquote(s string) (string, string)
func Unquote(s string) (string, error) {
	v, text := unquote(s)
	if text != "" {
		return v, errors.New(text)
	}
	return v, nil
}
