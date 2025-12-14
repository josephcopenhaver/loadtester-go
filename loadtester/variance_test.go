package loadtester

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/josephcopenhaver/csv-go/v3"
)

func Test_csvFmtLatencyVarianceAsInt64(t *testing.T) {
	toStr := func(f float64) string {

		var buf bytes.Buffer

		cw, err := csv.NewWriter(
			csv.WriterOpts().Writer(&buf),
			csv.WriterOpts().NumFields(1),
		)
		if err != nil {
			panic(fmt.Errorf("failed to create CSV writer: %w", err))
		}
		defer cw.Close()

		rw := cw.MustNewRecord()

		csvFmtLatencyVarianceAsInt64(rw, f)
		if _, err := rw.Write(); err != nil {
			panic(fmt.Errorf("failed to write CSV record: %w", err))
		}

		resp := strings.TrimRight(buf.String(), "\n")
		if resp == `""` {
			resp = ""
		}
		return resp
	}

	{
		exp := "9223372036854775807"
		if v := toStr(math.MaxFloat64); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		exp := "9223372036854775807"
		if v := toStr(math.Inf(1)); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		// garbage in - garbage out case
		exp := "0"
		if v := toStr(math.Inf(-1)); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		exp := "1"
		if v := toStr(1.0); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		// garbage in - garbage out case
		exp := "0"
		if v := toStr(-1.0); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
	{
		exp := ""
		if v := toStr(math.NaN()); v != exp {
			t.Fatalf("expected %s string, got %s", exp, v)
		}
	}
}
