package log

import "fmt"

type LoggingContext interface {
	AddLog(str string)
}

func Print(ctx LoggingContext, v ...any)                 { ctx.AddLog(fmt.Sprint(v...)) }
func Println(ctx LoggingContext, v ...any)               { ctx.AddLog(fmt.Sprintln(v...)) }
func Printf(ctx LoggingContext, format string, v ...any) { ctx.AddLog(fmt.Sprintf(format, v...)) }
