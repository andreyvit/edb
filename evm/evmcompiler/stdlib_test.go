package evmcompiler_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"log"
	"math"
	"runtime"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

func runGoText(t *testing.T, source string) string {
	t.Helper()
	vm := compileGoVM(t, source)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
		t.Fatal(err)
	}
	return out[0].AsString()
}
func TestGoStandardLibraryBasics(t *testing.T) {
	source := `import("fmt";"math";"strings";"strconv";"errors");func Run()string{n,err:=strconv.Atoi("41");if err!=nil{panic(err)};e:=errors.New("bad");return fmt.Sprintf("%d %.2f %s %v",n+1,math.Sqrt(2),strings.ToUpper("Hello"),errors.Is(fmt.Errorf("wrapped: %w",e),e))}`
	if got := runGoText(t, source); got != "42 1.41 HELLO true" {
		t.Fatal(got)
	}
}
func TestGoFormatDirectives(t *testing.T) {
	for _, format := range []string{"%v", "%+v", "%#v", "%08d", "%x", "%#x", "%q", "%T", "%[2]d %[1]s", "%*.*f", "%% %s", "%v %v", "%.3s", "%+08.2f", "%[9]s", "%", "%z", "%s"} {
		t.Run(format, func(t *testing.T) {
			args := []any{"héllo", 42, 1.25}
			source := fmt.Sprintf(`import "fmt";func Run()string{return fmt.Sprintf(%q,"héllo",42,1.25)}`, format)
			got := runGoText(t, source)
			want := fmt.Sprintf(format, args...)
			if got != want {
				t.Fatalf("got %q want %q", got, want)
			}
		})
	}
}

type testString int

func (v testString) String() string { return fmt.Sprintf("n=%d", int(v)) }

type testGoString int

func (v testGoString) GoString() string { return "gopher" }

type testFormat int

func (v testFormat) Format(s fmt.State, verb rune) {
	w, ok := s.Width()
	p, pk := s.Precision()
	fmt.Fprintf(s, "verb=%c w=%d/%t p=%d/%t +=%t", verb, w, ok, p, pk, s.Flag('+'))
}
func TestGoFormatMethodsAndCollections(t *testing.T) {
	prefix := `import "fmt";type S int;func(v S)String()string{return fmt.Sprintf("n=%d",int(v))};type G int;func(G)GoString()string{return "gopher"};type F int;func(F)Format(s fmt.State,verb rune){w,ok:=s.Width();p,pk:=s.Precision();s.Write([]byte(fmt.Sprintf("verb=%c w=%d/%t p=%d/%t +=%t",verb,w,ok,p,pk,s.Flag('+'))))};`
	tests := []struct {
		format, expr string
		native       any
		want         string
	}{
		{"%v", "[]S{2,3}", []testString{2, 3}, ""},
		{"%v", "map[string]S{\"b\":2,\"a\":1}", map[string]testString{"b": 2, "a": 1}, ""},
		{"%#v", "G(1)", testGoString(1), ""},
		{"%+8.2v", "F(1)", testFormat(1), ""},
		{"%+v", "struct{A S;b S}{1,2}", struct {
			A testString
			b testString
		}{1, 2}, ""},
		{"%q", "[]byte(\"hello\")", []byte("hello"), ""},
		{"%x", "[]byte{0,31,255}", []byte{0, 31, 255}, ""},
		{"%v", "[2]int{2,4}", [2]int{2, 4}, ""},
		{"%#v", "map[string]int(nil)", map[string]int(nil), ""},
		{"%#v", "[]int(nil)", []int(nil), ""},
		{"%#v", "(*int)(nil)", (*int)(nil), ""},
		{"%v", "&struct{N int}{4}", &struct{ N int }{4}, ""},
		{"%#v", "struct{N int}{4}", nil, "struct { N int }{N:4}"},
	}
	for _, tt := range tests {
		t.Run(tt.format+tt.expr, func(t *testing.T) {
			want := tt.want
			if want == "" {
				want = fmt.Sprintf(tt.format, tt.native)
			}
			got := runGoText(t, prefix+fmt.Sprintf(`func Run()string{return fmt.Sprintf(%q,%s)}`, tt.format, tt.expr))
			if got != want {
				t.Fatalf("got %q want %q", got, want)
			}
		})
	}
}
func TestGoFormatSpacingAndMethodsPanics(t *testing.T) {
	tests := []struct{ source, want string }{
		{`import "fmt";type S string;func Run()string{return fmt.Sprint(1,S("x"),2,3)+fmt.Sprintln(1,"x",2)}`, "1x2 31 x 2\n"},
		{`import "fmt";type T int;func(T)String()string{panic("oops")};func Run()string{return fmt.Sprint(T(1))}`, "%!v(PANIC=String method: oops)"},
		{`import "fmt";type T int;func(t *T)String()string{return fmt.Sprint(int(*t))};func Run()string{return fmt.Sprint((*T)(nil))}`, "<nil>"},
		{`import "fmt";func Run()string{return fmt.Sprintf("%[3]*.[2]*[1]f",1.2345,2,8)}`, "    1.23"},
	}
	for _, tt := range tests {
		if got := runGoText(t, tt.source); got != tt.want {
			t.Fatalf("got %q want %q", got, tt.want)
		}
	}
}

func TestGoStandardErrors(t *testing.T) {
	source := `import("fmt";"errors");type aliasError struct{target error};func(e aliasError)Error()string{return "alias"};func(e aliasError)Is(t error)bool{return t==e.target};func Run()string{a:=errors.New("a");b:=errors.New("b");w:=fmt.Errorf("%[2]w/%[1]w/%[2]w",a,b);list:=w.(interface{Unwrap()[]error}).Unwrap();j:=errors.Join(nil,a,b);return fmt.Sprintf("%v %v %v %v %v %v %v %s",errors.Is(w,a),errors.Is(w,b),len(list),list[0]==a,list[1]==b,errors.Is(aliasError{a},a),errors.Unwrap(fmt.Errorf("%w",a))==a,j.Error())}`
	if got := runGoText(t, source); got != "true true 2 true true true true a\nb" {
		t.Fatal(got)
	}
}
func TestGoStandardLogging(t *testing.T) {
	vm := compileGoVM(t, `import("fmt";"log");func Run(){defer println("done",3);print("raw",1);println("line",2);log.Print("log",1);log.Printf("value=%d",4);n,e:=fmt.Printf("hello %d",5);if n!=7||e!=nil{panic("output result")}}`)
	var text []string
	vm.Output = func(s string) error { text = append(text, s); return nil }
	if err := vm.Run(context.Background(), "Run", nil, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"raw1", "line 2\n", "log1\n", "value=4\n", "hello 5", "done 3\n"}
	if !slices.Equal(text, want) {
		t.Fatalf("%q", text)
	}
}
func TestGoStandardLibraryLimits(t *testing.T) {
	for _, test := range []struct {
		source string
		limit  evm.Limits
		want   error
	}{
		{`import "strings";func Run(){_=strings.Repeat("hello",1<<30)}`, evm.Limits{AllocationBytes: 1024, Steps: 1 << 63}, evm.ErrAllocation},
		{`import "fmt";func Run(){_=fmt.Sprintf("%1000000s","x")}`, evm.Limits{OutputBytes: 1000}, evm.ErrOutput},
		{`import "fmt";type T int;func(T)String()string{for{}};func Run(){_=fmt.Sprint(T(1))}`, evm.Limits{Steps: 2000}, evm.ErrSteps},
		{`import "errors";type T int;func(T)Error()string{return "loop"};func(t T)Unwrap()error{return t};func Run(){_=errors.Is(T(1),T(2))}`, evm.Limits{Steps: 2000}, evm.ErrSteps},
		{`import "fmt";func Run(){fmt.Print("12345");fmt.Print("67890")}`, evm.Limits{OutputBytes: 8}, evm.ErrOutput},
	} {
		vm := compileGoVM(t, test.source)
		vm.Limits = test.limit
		vm.Output = func(string) error { return nil }
		err := vm.Run(context.Background(), "Run", nil, nil)
		if !errors.Is(err, test.want) {
			t.Fatalf("%s: got %v want %v", test.source, err, test.want)
		}
	}
	for _, source := range []string{`import "fmt";func Run(){defer func(){recover()}();fmt.Print("x")}`, `import "log";func Run(){log.Println("x")}`} {
		vm := compileGoVM(t, source)
		want := errors.New("sink failed")
		vm.Output = func(string) error { return want }
		if err := vm.Run(context.Background(), "Run", nil, nil); !errors.Is(err, want) {
			t.Fatal(err)
		}
	}
}
func TestGoStandardPanics(t *testing.T) {
	for _, body := range []string{`strings.Repeat("x",-1)`, `strconv.FormatInt(1,1)`, `strconv.FormatFloat(1,'g',2,10)`} {
		src := `import("strings";"strconv");var _=strings.Repeat;var _=strconv.FormatInt;func Run()(ok bool){defer func(){ok=recover()!=nil}();_=` + body + `;return}`
		vm := compileGoVM(t, src)
		out := make([]values.Value, 1)
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
			t.Fatalf("%s: %v %v", body, err, out)
		}
	}
}
func TestGoUnusedStandardLibraries(t *testing.T) {
	bare, err := evmcompiler.Compile("main.go", `func Run()int{return 42}`)
	if err != nil {
		t.Fatal(err)
	}
	all, err := evmcompiler.Compile("main.go", `import(_ "fmt";_ "log";_ "math";_ "strings";_ "strconv";_ "errors");func Run()int{return 42}`)
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Functions) != len(bare.Functions) || len(all.Imports) != 0 || len(all.Globals) != 0 || len(all.Shapes) != len(bare.Shapes) || len(all.Types) != 0 {
		t.Fatalf("unused libraries grew module: bare %d shapes; all %d funcs, %d imports, %d globals, %d shapes, %d dynamic", len(bare.Shapes), len(all.Functions), len(all.Imports), len(all.Globals), len(all.Shapes), len(all.Types))
	}
	for _, src := range []string{`func Run(){fmt.Println(1)}`, `import "os";func Run(){os.Exit(0)}`, `import "fmt";func Run(){fmt.scalar(nil,"",0,0,false,false,'v')}`, `import "log";func Run(){log.Fatal("no")}`, `import "errors";func Run(){errors.As(nil,nil)}`, `import "evm/debug";func Run(){}`} {
		if _, err := evmcompiler.Compile("main.go", src); err == nil {
			t.Fatalf("accepted %s", src)
		}
	}
}

func TestGoMathLibrary(t *testing.T) {
	tests := []struct {
		expr string
		want float64
	}{
		{"Abs(-2)", math.Abs(-2)}, {"Acos(.2)", math.Acos(.2)}, {"Acosh(2)", math.Acosh(2)}, {"Asin(.2)", math.Asin(.2)}, {"Asinh(2)", math.Asinh(2)}, {"Atan(.2)", math.Atan(.2)}, {"Atan2(2,3)", math.Atan2(2, 3)}, {"Atanh(.2)", math.Atanh(.2)},
		{"Cbrt(2)", math.Cbrt(2)}, {"Ceil(2.1)", math.Ceil(2.1)}, {"Copysign(2,-3)", math.Copysign(2, -3)}, {"Cos(2)", math.Cos(2)}, {"Cosh(2)", math.Cosh(2)}, {"Dim(2,3)", math.Dim(2, 3)}, {"Erf(2)", math.Erf(2)}, {"Erfc(2)", math.Erfc(2)},
		{"Exp(2)", math.Exp(2)}, {"Exp2(2)", math.Exp2(2)}, {"Expm1(2)", math.Expm1(2)}, {"Floor(2.1)", math.Floor(2.1)}, {"FMA(2,3,4)", math.FMA(2, 3, 4)}, {"Hypot(2,3)", math.Hypot(2, 3)}, {"Inf(-1)", math.Inf(-1)},
		{"Log(2)", math.Log(2)}, {"Log10(2)", math.Log10(2)}, {"Log1p(2)", math.Log1p(2)}, {"Log2(2)", math.Log2(2)}, {"Max(2,3)", math.Max(2, 3)}, {"Min(2,3)", math.Min(2, 3)}, {"Mod(5,2)", math.Mod(5, 2)}, {"NaN()", math.NaN()},
		{"Nextafter(2,3)", math.Nextafter(2, 3)}, {"Pow(2,3)", math.Pow(2, 3)}, {"Pow10(2)", math.Pow10(2)}, {"Remainder(5,2)", math.Remainder(5, 2)}, {"Round(2.5)", math.Round(2.5)}, {"RoundToEven(2.5)", math.RoundToEven(2.5)},
		{"Sin(2)", math.Sin(2)}, {"Sinh(2)", math.Sinh(2)}, {"Sqrt(2)", math.Sqrt(2)}, {"Tan(2)", math.Tan(2)}, {"Tanh(2)", math.Tanh(2)}, {"Trunc(2.8)", math.Trunc(2.8)},
		{"Pi", math.Pi}, {"E", math.E}, {"Phi", math.Phi}, {"Sqrt2", math.Sqrt2}, {"SqrtE", math.SqrtE}, {"SqrtPi", math.SqrtPi}, {"SqrtPhi", math.SqrtPhi}, {"Ln2", math.Ln2}, {"Log2E", math.Log2E}, {"Ln10", math.Ln10}, {"Log10E", math.Log10E}, {"MaxFloat32", math.MaxFloat32}, {"MaxFloat64", math.MaxFloat64}, {"SmallestNonzeroFloat32", math.SmallestNonzeroFloat32}, {"SmallestNonzeroFloat64", math.SmallestNonzeroFloat64},
	}
	for _, tt := range tests {
		vm := compileGoVM(t, `import "math";func Run()float64{return math.`+tt.expr+`}`)
		out := make([]values.Value, 1)
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
			t.Fatal(err)
		}
		if got := out[0].AsFloat(); math.Float64bits(got) != math.Float64bits(tt.want) && !(math.IsNaN(got) && math.IsNaN(tt.want)) {
			t.Fatalf("%s: %v want %v", tt.expr, got, tt.want)
		}
	}
	src := `import "math";func Run()bool{a,b:=math.Modf(2.5);s,c:=math.Sincos(0);return a==2&&b==.5&&s==0&&c==1&&math.IsNaN(math.NaN())&&math.IsInf(math.Inf(1),1)&&math.Signbit(-1)&&math.Float32bits(math.Float32frombits(0x7fc00001))==0x7fc00001&&math.Float64bits(math.Float64frombits(0x7ff8000000000001))==0x7ff8000000000001}`
	vm := compileGoVM(t, src)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("math tuples/bits: %v %v", out, err)
	}
}
func TestGoStringAndConversionLibrary(t *testing.T) {
	tests := []struct{ expr, want string }{
		{`strings.Clone("a")`, "a"}, {`strings.Compare("a","b")`, "-1"}, {`strings.Contains("abc","bc")`, "true"}, {`strings.ContainsAny("abc","dx")`, "false"}, {`strings.ContainsRune("hé",'é')`, "true"}, {`strings.Count("aba","a")`, "2"},
		{`strings.EqualFold("K","K")`, "true"}, {`strings.Fields(" a\t b ")`, "[a b]"}, {`strings.HasPrefix("abc","ab")`, "true"}, {`strings.HasSuffix("abc","bc")`, "true"}, {`strings.Index("abc","b")`, "1"}, {`strings.IndexAny("abc","cz")`, "2"}, {`strings.IndexByte("abc",'b')`, "1"}, {`strings.IndexRune("hé",'é')`, "1"},
		{`strings.Join([]string{"a","b"},"!")`, "a!b"}, {`strings.LastIndex("ababa","ba")`, "3"}, {`strings.LastIndexAny("ababa","bc")`, "3"}, {`strings.LastIndexByte("ababa",'b')`, "3"}, {`strings.Repeat("é",2)`, "éé"}, {`strings.Replace("aaa","a","xy",2)`, "xyxya"}, {`strings.ReplaceAll("aaa","a","x")`, "xxx"},
		{`strings.Split("a,b,c",",")`, "[a b c]"}, {`strings.SplitAfter("a,b,c",",")`, "[a, b, c]"}, {`strings.SplitN("a,b,c",",",2)`, "[a b,c]"}, {`strings.SplitAfterN("a,b,c",",",2)`, "[a, b,c]"},
		{`strings.ToLower("ÄBC")`, "äbc"}, {`strings.ToUpper("äbc")`, "ÄBC"}, {`strings.ToTitle("abc")`, "ABC"}, {`strings.Trim("--a--","-")`, "a"}, {`strings.TrimLeft("--a--","-")`, "a--"}, {`strings.TrimRight("--a--","-")`, "--a"}, {`strings.TrimPrefix("abca","ab")`, "ca"}, {`strings.TrimSuffix("abca","ca")`, "ab"}, {`strings.TrimSpace("\u2003a\n")`, "a"},
		{`strconv.Itoa(-31)`, "-31"}, {`strconv.FormatBool(true)`, "true"}, {`strconv.FormatInt(-31,16)`, "-1f"}, {`strconv.FormatUint(31,16)`, "1f"}, {`strconv.FormatFloat(1.25,'f',1,64)`, "1.2"}, {`strconv.Quote("é\n")`, "\"é\\n\""}, {`strconv.QuoteToASCII("é")`, "\"\\u00e9\""},
	}
	for _, tt := range tests {
		src := `import("fmt";"strings";"strconv");var _=strings.Clone;var _=strconv.Itoa;func Run()string{return fmt.Sprint(` + tt.expr + `)}`
		if got := runGoText(t, src); got != tt.want {
			t.Fatalf("%s: got %q want %q", tt.expr, got, tt.want)
		}
	}
	src := `import("strings";"strconv";"fmt");func Run()string{a,b,c:=strings.Cut("a:b",":");p,ok:=strings.CutPrefix("abc","a");s,ok2:=strings.CutSuffix("abc","c");i,e:=strconv.ParseInt("-ff",16,64);u,e2:=strconv.ParseUint("ff",16,64);f,e3:=strconv.ParseFloat("1.25",64);t,e4:=strconv.ParseBool("TRUE");q,e5:=strconv.Unquote("\"hello\"");_,bad:=strconv.Atoi("oops");return fmt.Sprint(a,b,c,p,ok,s,ok2,i,e==nil,u,e2==nil,f,e3==nil,t,e4==nil,q,e5==nil,bad!=nil)}`
	if got := runGoText(t, src); got != "abtruebctrueabtrue -255 true 255 true 1.25 true true truehellotrue true" {
		t.Fatal(got)
	}
}
func TestGoIndependentSessionOutput(t *testing.T) {
	m, err := evmcompiler.Compile("main.go", `import "fmt";func Run(){fmt.Println("hello")}`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	a, b := evm.New(p), evm.New(p)
	var first, second string
	a.Output = func(s string) error { first += s; return nil }
	b.Output = func(s string) error { second += s; return nil }
	for _, vm := range []*evm.VM{a, b, a} {
		if err := vm.Run(context.Background(), "Run", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if first != "hello\nhello\n" || second != "hello\n" {
		t.Fatalf("%q / %q", first, second)
	}
}
func BenchmarkStandardLibraryUnusedStartup(b *testing.B) {
	for _, imports := range []string{"", `import(_ "fmt";_ "log";_ "math";_ "strings";_ "strconv";_ "errors");`} {
		b.Run(fmt.Sprintf("imports=%t", imports != ""), func(b *testing.B) {
			m, err := evmcompiler.Compile("main.go", imports+`func Run()int{return 42}`)
			if err != nil {
				b.Fatal(err)
			}
			p, err := evm.Link(m)
			if err != nil {
				b.Fatal(err)
			}
			out := make([]values.Value, 1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vm := evm.New(p)
				if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestGoFormatCompatibility(t *testing.T) {
	vm := compileGoVM(t, `import "fmt";func Run(format string)string{return fmt.Sprintf(format,"héllo",3,1.25,true,[]int{2,4},[]byte("a\n"),nil,complex(2,3))}`)
	args := []any{"héllo", 3, 1.25, true, []int{2, 4}, []byte("a\n"), nil, complex(2, 3)}
	formats := []string{"%w", "%[7]w", "%[0]v", "%[99]v", "%[a]v", "%[2]*v", "%.[2]*v", "%[2]*.[2]*[3]f", "%[2]3d", "%[2].3d", "%[2]*% %v", "%.s", "%☃", "%[2]", "%[1]v %v %v", "%[1]v %v %v %v %v %v %v %v"}
	for i := 1; i <= len(args); i++ {
		for _, verb := range "vTtbcdoOqxXUeEfFgGsp" {
			if verb == 'p' && (i == 5 || i == 6) {
				continue
			}
			for _, flags := range []string{"", "#", "+", "-", " ", "0", "#0"} {
				formats = append(formats, fmt.Sprintf("%%%s8.3[%d]%c", flags, i, verb))
			}
		}
	}
	out := make([]values.Value, 1)
	for _, format := range formats {
		if err := vm.Reset(); err != nil {
			t.Fatal(err)
		}
		if err := vm.Run(context.Background(), "Run", []values.Value{values.String(format)}, out); err != nil {
			t.Fatalf("%q: %v", format, err)
		}
		got, want := out[0].AsString(), fmt.Sprintf(format, args...)
		if got != want {
			t.Errorf("%q: got %q want %q", format, got, want)
			if t.Failed() {
				return
			}
		}
	}
}

func FuzzStandardFormatting(f *testing.F) {
	for _, s := range []string{"%v", "%[7]w", "%8.3[6]v", "%[2]*.[2]*[3]f", "%[2]3d", "%% %s", "%"} {
		f.Add(s)
	}
	m, err := evmcompiler.Compile("main.go", `import "fmt";func Run(format string)string{return fmt.Sprintf(format,"héllo",3,1.25,true,[]int{2,4},[]byte("a\n"),nil,complex(2,3))}`)
	if err != nil {
		f.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		f.Fatal(err)
	}
	native := []any{"héllo", 3, 1.25, true, []int{2, 4}, []byte("a\n"), nil, complex(2, 3)}
	f.Fuzz(func(t *testing.T, format string) {
		if len(format) > 100 || strings.ContainsRune(format, 'p') {
			t.Skip()
		}
		n := 0
		for _, r := range format {
			if r >= '0' && r <= '9' {
				n = n*10 + int(r-'0')
				if n > 1000 {
					t.Skip()
				}
			} else {
				n = 0
			}
		}
		vm := evm.New(p)
		vm.Limits.Steps = 100000
		vm.Limits.OutputBytes = 4096
		out := make([]values.Value, 1)
		err := vm.Run(context.Background(), "Run", []values.Value{values.String(format)}, out)
		if errors.Is(err, evm.ErrSteps) || errors.Is(err, evm.ErrOutput) || errors.Is(err, evm.ErrAllocation) {
			t.Skip()
		}
		if err != nil {
			t.Fatal(err)
		}
		got, want := out[0].AsString(), fmt.Sprintf(format, native...)
		if got != want {
			t.Fatalf("%q: got %q want %q", format, got, want)
		}
	})
}

func TestGoStandardReviewRegressions(t *testing.T) {
	source := `type Inner struct{N int};type Outer struct{I Inner};func Run()any{return Outer{}}`
	a, err := evmcompiler.Compile("main.go", source)
	if err != nil {
		t.Fatal(err)
	}
	b, err := evmcompiler.Compile("main.go", `import _ "fmt";`+source)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Functions) != len(b.Functions) || len(a.Types) != len(b.Types) || len(a.Shapes) != len(b.Shapes) {
		t.Fatal("blank fmt import discovers formatted children")
	}
	for _, src := range []string{`func Run(){print("12345")}`, `func Run(){println("1234")}`} {
		vm := compileGoVM(t, src)
		vm.Limits.OutputBytes = 5
		var got string
		vm.Output = func(s string) error { got = s; return nil }
		if err := vm.Run(context.Background(), "Run", nil, nil); err != nil || len(got) != 5 {
			t.Fatalf("%s: %q, %v", src, got, err)
		}
	}
	for _, expr := range []string{`map[complex64]int{2+3i:1,1+9i:2,1+2i:3}`, `map[complex128]int{2+3i:1,1+9i:2,1+2i:3}`} {
		got := runGoText(t, `import "fmt";func Run()string{return fmt.Sprint(`+expr+`)}`)
		want := fmt.Sprint(map[complex128]int{2 + 3i: 1, 1 + 9i: 2, 1 + 2i: 3})
		if got != want {
			t.Fatalf("%q want %q", got, want)
		}
	}
	got := runGoText(t, `import "fmt";type Pair[A,B any]struct{A A;B B};func Run()string{return fmt.Sprintf("%T|%#v",Pair[int,string]{},Pair[int,string]{})}`)
	if got != `plugin.Pair[int,string]|plugin.Pair[int,string]{A:0, B:""}` {
		t.Fatal(got)
	}
}
func TestGoStandardImportsCannotBeOverridden(t *testing.T) {
	m, err := evmcompiler.Compile("main.go", `import "math";func Run()float64{return math.Sqrt(4)}`)
	if err != nil {
		t.Fatal(err)
	}
	pkg := evm.Package{Path: "math", Functions: []evm.ExternalFunction{{Name: "Sqrt", Signature: m.Imports[0].Signature, Call: func(*evm.Call) error { return nil }}}}
	if _, err := evm.Link(m, pkg); err == nil {
		t.Fatal("overrode standard primitive")
	}
	if _, err := evmcompiler.Compile("main.go", `import "math";func Run()float64{return math.Sqrt(4)}`, pkg); err == nil {
		t.Fatal("overrode virtual package")
	}
	m.Imports[0].Signature.Params = nil
	if _, err := evm.Link(m); err == nil {
		t.Fatal("accepted wrong virtual native ABI")
	}
}
func TestGoDefaultLoggerPreservesNewlines(t *testing.T) {
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	defer func() { log.SetOutput(oldWriter); log.SetFlags(oldFlags); log.SetPrefix(oldPrefix) }()
	var buffer bytes.Buffer
	log.SetOutput(&buffer)
	log.SetFlags(0)
	log.SetPrefix("")
	vm := compileGoVM(t, `import "fmt";func Run(){fmt.Print("x\n\n")}`)
	if err := vm.Run(context.Background(), "Run", nil, nil); err != nil {
		t.Fatal(err)
	}
	if got := buffer.String(); got != "x\n\n" {
		t.Fatalf("%q", got)
	}
}

func TestGoFormattingBudgets(t *testing.T) {
	vm := compileGoVM(t, `import "fmt";func Run()string{return fmt.Sprintf("%.100s","x")}`)
	vm.Limits.OutputBytes = 10
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].AsString() != "x" {
		t.Fatalf("precision cap: %v %v", out, err)
	}
	vm = compileGoVM(t, `import "fmt";func Run(text string)string{return fmt.Sprintf("%s",text)}`)
	text := strings.Repeat("x", 100000)
	if err := vm.Run(context.Background(), "Run", []values.Value{values.String(text)}, out); err != nil || out[0].AsString() != text {
		t.Fatalf("medium formatting: %v", err)
	}
	vm = compileGoVM(t, `import "fmt";func Run()string{return fmt.Sprintf("%1500000s","x")}`)
	vm.Limits.OutputBytes = 2 << 20
	vm.Limits.Steps = 20_000_000
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || len(out[0].AsString()) != 1500000 {
		t.Fatalf("large literal width: %v", err)
	}
	if got := runGoText(t, `import "fmt";func Run()string{return fmt.Sprintf("%*s",1500000,"x")}`); got != fmt.Sprintf("%*s", 1500000, "x") {
		t.Fatalf("Go star-width limit: %q", got)
	}
}
func TestGoStandardPanicValueTypes(t *testing.T) {
	tests := []struct{ statement, want string }{
		{`_ = strings.Repeat("x",-1)`, "strings: negative Repeat count"},
		{`defer strings.Repeat("x",-1)`, "strings: negative Repeat count"},
		{`_ = strconv.FormatInt(2,1)`, "strconv: illegal AppendInt/FormatInt base"},
		{`defer strconv.FormatInt(2,1)`, "strconv: illegal AppendInt/FormatInt base"},
	}
	for _, tt := range tests {
		src := `import("strings";"strconv");var _=strings.Repeat;var _=strconv.FormatInt;func Run()(s string){defer func(){s=recover().(string)}();` + tt.statement + `;return}`
		if got := runGoText(t, src); got != tt.want {
			t.Fatalf("%s: %q", tt.statement, got)
		}
	}
	vm := compileGoVM(t, `import "strings";func Run(){strings.Repeat("x",-1)}`)
	err := vm.Run(context.Background(), "Run", nil, nil)
	var p *evm.PanicError
	if !errors.As(err, &p) || p.Type != values.StringType || p.Value.AsString() != "strings: negative Repeat count" {
		t.Fatalf("uncaught standard panic: %v", err)
	}
}

func TestGoFormattingNativeInterfaceChildrenAndGC(t *testing.T) {
	number := &values.Shape{Type: values.NewType("h.Number", values.Raw), Kind: values.IntKind, Bits: 64, Cells: 1}
	object := &values.Shape{Type: values.NewType("h.Object", values.Pointer), Kind: values.StructKind, Cells: 1, Fields: []values.Field{{Name: "N", Shape: number}}}
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "Number", Shape: number}, {Name: "Object", Shape: object}}, Methods: []evm.ExternalMethod{{Name: "String", Receiver: number.Type, Signature: evm.Signature{Results: []*values.Type{values.StringType}}, Call: func(c *evm.Call) error { c.SetValue(0, values.String("native")); return nil }}}, Functions: []evm.ExternalFunction{
		{Name: "Make", Signature: evm.Signature{Results: []*values.Type{values.AnyType}}, DynamicTypes: []*values.Type{object.Type}, Call: func(c *evm.Call) error {
			v := values.Int(3)
			return c.SetInterface(0, object.Type, values.Value{P: unsafe.Pointer(&v)})
		}},
		{Name: "GC", Call: func(*evm.Call) error { runtime.GC(); return nil }},
	}}
	source := `import("fmt";"strings";"h");func Run()string{parts:=strings.Split(strings.Repeat("ab,",20),",");h.GC();return fmt.Sprint(h.Make())+strings.Join(parts,"-")}`
	m, err := evmcompiler.Compile("main.go", source, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	if got, want := out[0].AsString(), "{native}"+strings.Repeat("ab-", 20); got != want {
		t.Fatalf("%q want %q", got, want)
	}
}
func TestGoNativeLibraryOperandSizeDoesNotSpendSteps(t *testing.T) {
	vm := compileGoVM(t, `import "strings"; func Run(s string) int { return strings.Count(s, "x") }`)
	vm.Limits.Steps = 32
	for _, n := range []int{1, 100000} {
		out := make([]values.Value, 1)
		if err := vm.Run(context.Background(), "Run", []values.Value{values.String(strings.Repeat("x", n))}, out); err != nil || out[0].AsInt() != int64(n) {
			t.Fatalf("Count(%d bytes) = %v, %v", n, out, err)
		}
	}
}
func TestGoFormattingTypeAndPointerSkipsContents(t *testing.T) {
	cells := make([]values.Value, 100000)
	slice := values.SliceValue(unsafe.Pointer(&cells[0]), uint32(len(cells)), uint32(len(cells)))
	for _, tt := range []struct {
		src  string
		arg  values.Value
		want string
	}{
		{`import "fmt";func Run(a []byte)string{return fmt.Sprintf("%T",a)}`, slice, "[]uint8"},
		{`import "fmt";func Run(a string)string{return fmt.Sprintf("%T",a)}`, values.String(strings.Repeat("x", 100000)), "string"},
		{`import "fmt";func Run(a []int)string{return fmt.Sprintf("%p",a)}`, slice, "0x"},
	} {
		vm := compileGoVM(t, tt.src)
		vm.Limits.Steps = 10000
		vm.Limits.AllocationBytes = 1 << 20
		out := make([]values.Value, 1)
		if err := vm.Run(context.Background(), "Run", []values.Value{tt.arg}, out); err != nil {
			t.Fatal(err)
		}
		if got := out[0].AsString(); !strings.HasPrefix(got, tt.want) {
			t.Fatalf("%q want %q", got, tt.want)
		}
	}
}

func TestGoInvalidPointerFormatting(t *testing.T) {
	for _, format := range []string{"%p", "%#p", "%+p", "%20p", "%#20p"} {
		native := struct {
			N int
			F float64
			S string
			P *int
		}{2, 1.25, "hi", nil}
		src := fmt.Sprintf(`import "fmt";func Run()string{return fmt.Sprintf(%q,struct{N int;F float64;S string;P *int}{2,1.25,"hi",nil})}`, format)
		if got, want := runGoText(t, src), fmt.Sprintf(format, native); got != want {
			t.Fatalf("%s: %q want %q", format, got, want)
		}
	}
}
