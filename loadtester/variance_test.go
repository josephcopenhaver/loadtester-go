package loadtester

import (
	"math"
	"testing"
)

func Test_varianceFloatString(t *testing.T) {
	{
		exp := "9223372036854775807"
		if v := varianceFloatString(math.MaxFloat64); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		exp := "9223372036854775807"
		if v := varianceFloatString(math.Inf(1)); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		// garbage in - garbage out case
		exp := "-9223372036854775808"
		if v := varianceFloatString(math.Inf(-1)); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		exp := "1"
		if v := varianceFloatString(1.0); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		// garbage in - garbage out case
		exp := "-1"
		if v := varianceFloatString(-1.0); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		exp := ""
		if v := varianceFloatString(math.NaN()); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
}
