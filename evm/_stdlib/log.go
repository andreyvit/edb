package log

import "fmt"

func Print(v ...any)   { fmt.Println(fmt.Sprint(v...)) }
func Println(v ...any) { fmt.Print(fmt.Sprintln(v...)) }
func Printf(format string, v ...any) {
	s := fmt.Sprintf(format, v...)
	if len(s) == 0 || s[len(s)-1] != '\n' {
		s += "\n"
	}
	fmt.Print(s)
}
