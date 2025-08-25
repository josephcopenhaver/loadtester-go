package csv

// this csv package is massively reduced down to just what I need to construct a metrics file
// and its contents without creating additional allocations
//
// it is not general purpose nor is it meant to be copied for reuse in any fashion
//
// String type is not fully implemented nor does it cover all edge cases it should
//
// float32 and signed integer types have also been commented out then deleted
// (see previous commit)

import (
	"io"
	"math"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

const (
	maxBufSize      = 30
	fieldSeparator  = byte(',')
	recordSeparator = byte('\n')
)

var (
	fieldSeparatorBytes = []byte{fieldSeparator}
	fieldSeparatorStr   = string(fieldSeparator)

	recordSeparatorBytes = []byte{recordSeparator}
	recordSeparatorStr   = string(recordSeparator)
)

type fieldEncoder struct {
	io.Writer
}

type fType uint8

const (
	fTypeRaw fType = iota
	fTypeStr
	fTypeUNum
	fTypeF64Num
	fTypeTime
)

type Field struct {
	// "_ [0]func()" makes the struct non-comparable
	_ [0]func()

	raw   []byte
	str   string
	num   uint64
	fType fType
}

var bufPool = sync.Pool{
	New: func() any {
		return &[maxBufSize]byte{}
	},
}

func tmpBuf() *[maxBufSize]byte {
	return bufPool.Get().(*[maxBufSize]byte)
}

func putTmpBuf(v *[maxBufSize]byte, numWritten int) {
	{
		p := (*v)[:numWritten]

		for i := range numWritten {
			p[i] = 0
		}
	}

	bufPool.Put(v)
}

// MarshalCSVFieldTo is not a full general implementation of how
// to serialize all primitive and string types to a csv field.
//
// it should not be copied by anyone except for similar cases
// when serializing mostly numbers and csv header strings
func (f *Field) MarshalCSVFieldTo(fe *fieldEncoder) error {
	switch f.fType {
	case fTypeRaw:
		if len(f.raw) == 0 {
			return nil
		}

		_, err := fe.Write(f.raw)
		return err
	case fTypeStr:
		s := f.str
		if s == "" {
			return nil
		}

		// intentionally not quoting nor checking for sequences that require escaping or field quoting
		// THIS IS NOT A FULL GENERAL IMPLEMENTATION

		if v, ok := fe.Writer.(io.StringWriter); ok {
			_, err := v.WriteString(s)
			return err
		}

		b := unsafe.Slice(unsafe.StringData(s), len(s))

		_, err := fe.Write(b)
		return err
	case fTypeUNum:
		// in worst case: requires 20 bytes to render

		tb := tmpBuf()
		tbn := cap(*tb)

		{
			tb := tb
			defer func() {
				putTmpBuf(tb, tbn)
			}()
		}

		b := (*tb)[:0]

		b = strconv.AppendUint(b, f.num, 10)
		tbn = len(b)

		_, err := fe.Write(b)
		return err
	case fTypeF64Num:
		// in worst case: requires 24 bytes to render // "-1.7976931348623157e+308"

		n := math.Float64frombits(f.num)

		tb := tmpBuf()
		tbn := cap(*tb)

		{
			tb := tb
			defer func() {
				putTmpBuf(tb, tbn)
			}()
		}

		b := (*tb)[:0]

		b = strconv.AppendFloat(b, n, 'g', -1, 64)
		tbn = len(b)

		_, err := fe.Write(b)
		return err
	case fTypeTime:
		// in worst case: requires 30 bytes to render

		ts := time.Unix(0, int64(f.num)).UTC()

		tb := tmpBuf()
		tbn := cap(*tb)

		{
			tb := tb
			defer func() {
				putTmpBuf(tb, tbn)
			}()
		}

		b := (*tb)[:0]

		b = ts.AppendFormat(b, time.RFC3339Nano)
		tbn = len(b)

		_, err := fe.Write(b)
		return err
	default:
		panic("unknown csv field type")
	}
}

// Raw should be used sparingly just to defer
// the serialization method to the calling context
// such that there are no allocations and the slice
// will not be modified before it is copied to the
// destination buffer
func Raw(v []byte) Field {
	return Field{
		raw:   v,
		fType: fTypeRaw,
	}
}

func Str(v string) Field {
	return Field{
		str:   v,
		fType: fTypeStr,
	}
}

func Uint[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](v T) Field {
	return Field{
		num:   uint64(v),
		fType: fTypeUNum,
	}
}

// Float64 will render an empty string when written to a writer
// if the float value is NaN
func Float64(v float64) Field {
	if math.IsNaN(v) {
		return Field{}
	}

	return Field{
		num:   math.Float64bits(v),
		fType: fTypeF64Num,
	}
}

func Time(v time.Time) Field {
	return Field{
		num:   uint64(v.UnixNano()),
		fType: fTypeTime,
	}
}

func (fe *fieldEncoder) writeFields(fields ...Field) error {
	if len(fields) == 0 {
		return nil
	}

	if err := fields[0].MarshalCSVFieldTo(fe); err != nil {
		return err
	}

	switch v := fe.Writer.(type) {
	case io.ByteWriter:
		for i := 1; i < len(fields); i++ {
			f := &fields[i]

			if err := v.WriteByte(fieldSeparator); err != nil {
				return err
			}

			if err := f.MarshalCSVFieldTo(fe); err != nil {
				return err
			}
		}
	case io.StringWriter:
		for i := 1; i < len(fields); i++ {
			f := &fields[i]

			if _, err := v.WriteString(fieldSeparatorStr); err != nil {
				return err
			}

			if err := f.MarshalCSVFieldTo(fe); err != nil {
				return err
			}
		}
	default:
		for i := 1; i < len(fields); i++ {
			f := &fields[i]

			if _, err := v.Write(fieldSeparatorBytes); err != nil {
				return err
			}

			if err := f.MarshalCSVFieldTo(fe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (fe *fieldEncoder) writeRow(fields ...Field) error {
	if err := fe.writeFields(fields...); err != nil {
		return err
	}

	switch v := fe.Writer.(type) {
	case io.ByteWriter:
		return v.WriteByte(recordSeparator)
	case io.StringWriter:
		_, err := v.WriteString(recordSeparatorStr)
		return err
	default:
		_, err := v.Write(recordSeparatorBytes)
		return err
	}
}

func newEncoder(w io.Writer) *fieldEncoder {
	return &fieldEncoder{Writer: w}
}

func WriteRow(w io.Writer, fields ...Field) error {
	return newEncoder(w).writeRow(fields...)
}
