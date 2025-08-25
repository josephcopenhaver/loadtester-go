package csv

import (
	"io"
	"testing"
	"time"
)

func newFE() *fieldEncoder { return newEncoder(io.Discard) }

//
// Zero-allocation assertions (fast fail)
//

func assertZeroAllocs(t *testing.T, name string, fn func()) {
	t.Helper()

	allocs := testing.AllocsPerRun(1000, fn)
	if allocs > 0 {
		t.Fatalf("%s: expected 0 heap allocations, got %0.2f", name, allocs)
	}
}

func TestAllocs_Marshal_Raw_Empty(t *testing.T) {
	fe := newFE()
	f := Raw(nil)
	assertZeroAllocs(t, "Raw(nil)", func() {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_Marshal_Raw_Small(t *testing.T) {
	fe := newFE()
	f := Raw([]byte("ok"))
	assertZeroAllocs(t, "Raw(\"ok\")", func() {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_Marshal_Str_Empty(t *testing.T) {
	fe := newFE()
	f := Str("")
	assertZeroAllocs(t, "Str(\"\")", func() {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_Marshal_Str_Short(t *testing.T) {
	fe := newFE()
	f := Str("abc")
	assertZeroAllocs(t, "Str(\"abc\")", func() {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_Marshal_Uint_Zero(t *testing.T) {
	fe := newFE()
	f := Uint(uint64(0))
	assertZeroAllocs(t, "Uint(0)", func() {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_Marshal_Uint_NonZero(t *testing.T) {
	fe := newFE()
	f := Uint(uint64(1234567890))
	assertZeroAllocs(t, "Uint(1234567890)", func() {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_Marshal_Float64(t *testing.T) {
	fe := newFE()
	f := Float64(12345.6789)
	assertZeroAllocs(t, "Float64(12345.6789)", func() {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_Marshal_Time(t *testing.T) {
	fe := newFE()
	ts := time.Unix(1_700_000_000, 123_456_789) // stable, post-epoch
	f := Time(ts)
	assertZeroAllocs(t, "Time", func() {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_WriteFields_Mixed(t *testing.T) {
	fe := newFE()
	ts := time.Unix(1_700_000_000, 0)
	fields := []Field{
		Str("svc"),
		Uint(uint32(42)),
		Float64(3.14159),
		Time(ts),
		Raw([]byte("ok")),
	}
	assertZeroAllocs(t, "WriteFields(mixed)", func() {
		if err := fe.writeFields(fields...); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_WriteRow_Mixed(t *testing.T) {
	fe := newFE()
	ts := time.Unix(1_700_000_000, 999_000)
	fields := []Field{
		Str("svc"),
		Uint(uint64(0)),
		Float64(2.718281828),
		Time(ts),
		Raw([]byte("done")),
	}
	assertZeroAllocs(t, "writeRow(mixed)", func() {
		if err := fe.writeRow(fields...); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAllocs_WriteRow_Mixed_NonZero(t *testing.T) {
	fe := newFE()
	ts := time.Unix(1_700_000_000, 999_000)
	fields := []Field{
		Str("svc"),
		Uint(uint64(1)),
		Float64(2.718281828),
		Time(ts),
		Raw([]byte("done")),
	}
	assertZeroAllocs(t, "writeRow(mixed-non-zero)", func() {
		if err := fe.writeRow(fields...); err != nil {
			t.Fatal(err)
		}
	})
}

//
// Benchmarks
//

func BenchmarkMarshal_Raw_Small(b *testing.B) {
	fe := newFE()
	f := Raw([]byte("ok"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal_Str_Short(b *testing.B) {
	fe := newFE()
	f := Str("abc")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal_Uint_Zero(b *testing.B) {
	fe := newFE()
	f := Uint(uint64(0))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal_Uint_NonZero(b *testing.B) {
	fe := newFE()
	f := Uint(uint64(18446744073709551615)) // max uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal_Float64_Normal(b *testing.B) {
	fe := newFE()
	f := Float64(12345.6789)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal_Time(b *testing.B) {
	fe := newFE()
	f := Time(time.Unix(1_700_000_000, 123_000_000))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := f.MarshalCSVFieldTo(fe); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteFields_5(b *testing.B) {
	fe := newFE()
	ts := time.Unix(1_700_000_000, 0)
	fields := []Field{
		Str("svc"),
		Uint(uint32(42)),
		Float64(3.14159),
		Time(ts),
		Raw([]byte("ok")),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fe.writeFields(fields...); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteRow_5(b *testing.B) {
	fe := newFE()
	ts := time.Unix(1_700_000_000, 999_000)
	fields := []Field{
		Str("svc"),
		Uint(uint64(0)),
		Float64(2.718281828),
		Time(ts),
		Raw([]byte("done")),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fe.writeRow(fields...); err != nil {
			b.Fatal(err)
		}
	}
}
