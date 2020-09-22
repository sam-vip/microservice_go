package strftime_test

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	envload "github.com/lestrrat-go/envload"
	"github.com/lestrrat-go/strftime"
	"github.com/stretchr/testify/assert"
)

var ref = time.Unix(1136239445, 123456789).UTC()

func TestExclusion(t *testing.T) {
	s, err := strftime.New("%p PM")
	if !assert.NoError(t, err, `strftime.New should succeed`) {
		return
	}

	var tm time.Time
	if !assert.Equal(t, "AM PM", s.FormatString(tm)) {
		return
	}
}

func TestInvalid(t *testing.T) {
	_, err := strftime.New("%")
	if !assert.Error(t, err, `strftime.New should return error`) {
		return
	}

	_, err = strftime.New(" %")
	if !assert.Error(t, err, `strftime.New should return error`) {
		return
	}
	_, err = strftime.New(" % ")
	if !assert.Error(t, err, `strftime.New should return error`) {
		return
	}
}

func TestFormat(t *testing.T) {
	l := envload.New()
	defer l.Restore()

	os.Setenv("LC_ALL", "C")

	s, err := strftime.Format(`%A %a %B %b %C %c %D %d %e %F %H %h %I %j %k %l %M %m %n %p %R %r %S %T %t %U %u %V %v %W %w %X %x %Y %y %Z %z`, ref)
	if !assert.NoError(t, err, `strftime.Format succeeds`) {
		return
	}

	if !assert.Equal(t, "Monday Mon January Jan 20 Mon Jan  2 22:04:05 2006 01/02/06 02  2 2006-01-02 22 Jan 10 002 22 10 04 01 \n PM 22:04 10:04:05 PM 05 22:04:05 \t 01 1 01  2-Jan-2006 01 1 22:04:05 01/02/06 2006 06 UTC +0000", s, `formatted result matches`) {
		return
	}
}

func TestFormatBlanks(t *testing.T) {
	l := envload.New()
	defer l.Restore()

	os.Setenv("LC_ALL", "C")

	{
		dt := time.Date(1, 1, 1, 18, 0, 0, 0, time.UTC)
		s, err := strftime.Format("%l", dt)
		if !assert.NoError(t, err, `strftime.Format succeeds`) {
			return
		}

		if !assert.Equal(t, " 6", s, "leading blank is properly set") {
			return
		}
	}
	{
		dt := time.Date(1, 1, 1, 6, 0, 0, 0, time.UTC)
		s, err := strftime.Format("%k", dt)
		if !assert.NoError(t, err, `strftime.Format succeeds`) {
			return
		}

		if !assert.Equal(t, " 6", s, "leading blank is properly set") {
			return
		}
	}
}

func TestFormatZeropad(t *testing.T) {
	l := envload.New()
	defer l.Restore()

	os.Setenv("LC_ALL", "C")

	{
		dt := time.Date(1, 1, 1, 1, 0, 0, 0, time.UTC)
		s, err := strftime.Format("%j", dt)
		if !assert.NoError(t, err, `strftime.Format succeeds`) {
			return
		}

		if !assert.Equal(t, "001", s, "padding is properly set") {
			return
		}
	}
	{
		dt := time.Date(1, 1, 10, 6, 0, 0, 0, time.UTC)
		s, err := strftime.Format("%j", dt)
		if !assert.NoError(t, err, `strftime.Format succeeds`) {
			return
		}

		if !assert.Equal(t, "010", s, "padding is properly set") {
			return
		}
	}
	{
		dt := time.Date(1, 6, 1, 6, 0, 0, 0, time.UTC)
		s, err := strftime.Format("%j", dt)
		if !assert.NoError(t, err, `strftime.Format succeeds`) {
			return
		}

		if !assert.Equal(t, "152", s, "padding is properly set") {
			return
		}
	}
	{
		dt := time.Date(100, 1, 1, 1, 0, 0, 0, time.UTC)
		s, err := strftime.Format("%C", dt)
		if !assert.NoError(t, err, `strftime.Format succeeds`) {
			return
		}

		if !assert.Equal(t, "01", s, "padding is properly set") {
			return
		}
	}
}

func TestGHIssue5(t *testing.T) {
	const expected = `apm-test/logs/apm.log.01000101`
	p, _ := strftime.New("apm-test/logs/apm.log.%Y%m%d")
	dt := time.Date(100, 1, 1, 1, 0, 0, 0, time.UTC)
	if !assert.Equal(t, expected, p.FormatString(dt), `patterns including 'pm' should be treated as verbatim formatter`) {
		return
	}
}

func TestGHPR7(t *testing.T) {
	const expected = `123`

	p, _ := strftime.New(`%L`, strftime.WithMilliseconds('L'))
	if !assert.Equal(t, expected, p.FormatString(ref), `patterns should match for custom specification`) {
		return
	}
}

func Example_CustomSpecifications() {
	{
		// I want %L as milliseconds!
		p, err := strftime.New(`%L`, strftime.WithMilliseconds('L'))
		if err != nil {
			fmt.Println(err)
			return
		}
		p.Format(os.Stdout, ref)
		os.Stdout.Write([]byte{'\n'})
	}

	{
		// I want %f as milliseconds!
		p, err := strftime.New(`%f`, strftime.WithMilliseconds('f'))
		if err != nil {
			fmt.Println(err)
			return
		}
		p.Format(os.Stdout, ref)
		os.Stdout.Write([]byte{'\n'})
	}

	{
		// I want %X to print out my name!
		a := strftime.Verbatim(`Daisuke Maki`)
		p, err := strftime.New(`%X`, strftime.WithSpecification('X', a))
		if err != nil {
			fmt.Println(err)
			return
		}
		p.Format(os.Stdout, ref)
		os.Stdout.Write([]byte{'\n'})
	}

	{
		// I want a completely new specification set, and I want %X to print out my name!
		a := strftime.Verbatim(`Daisuke Maki`)

		ds := strftime.NewSpecificationSet()
		ds.Set('X', a)
		p, err := strftime.New(`%X`, strftime.WithSpecificationSet(ds))
		if err != nil {
			fmt.Println(err)
			return
		}
		p.Format(os.Stdout, ref)
		os.Stdout.Write([]byte{'\n'})
	}

	// OUTPUT:
	// 123
	// 123
	// Daisuke Maki
	// Daisuke Maki
}

func TestGHIssue9(t *testing.T) {
	pattern, _ := strftime.New("/longer/path/to/see/where/this/leads//test99%Y%m%d.log")

	var buf bytes.Buffer
	pattern.Format(&buf, time.Now())

	if !assert.True(t, strings.Contains(buf.String(), "/")) {
		t.Logf("---> %s", buf.String())
		return
	}
}
