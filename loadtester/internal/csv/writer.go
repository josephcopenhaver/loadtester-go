package csv

import (
	"bytes"
	"io"
	"math"
	"strconv"
	"time"
	"unsafe"
)

var (
	emptyField = Field{}
)

type fieldEncoder struct {
	io.Writer
}

type tokenType uint8

const (
	tokenTypeRaw tokenType = iota
	tokenTypeStr
	// tokenTypeINum
	tokenTypeUNum
	tokenTypeF64Num
	// tokenTypeF32Num
	tokenTypeTime
)

type Field struct {
	// nonComparable // TODO: implement

	raw []byte

	str string

	num uint64

	tokenType tokenType
}

func (f *Field) MarshalCSVFieldTo(fe *fieldEncoder) error {
	var buf [30]byte

	switch f.tokenType {
	case tokenTypeRaw:
		if len(f.raw) == 0 {
			return nil
		}

		_, err := fe.Write(f.raw)
		return err
	case tokenTypeStr:
		v := f.str
		if v == "" {
			return nil
		}

		b := unsafe.Slice(unsafe.StringData(v), len(v))

		if i := bytes.IndexAny(b, "\",\n"); i != -1 {
			panic("not implemented")
		}

		_, err := fe.Write(b)
		return err
	// case tokenTypeINum:
	// 	if f.num == 0 {
	// 		_, err := fe.Write([]byte{'0'})
	// 		return err
	// 	}

	// 	// var buf [20]byte
	// 	b := strconv.AppendInt(buf[:0], int64(f.num), 10)

	// 	_, err := fe.Write(b)
	// 	return err
	case tokenTypeUNum:
		if f.num == 0 {
			_, err := fe.Write([]byte{'0'})
			return err
		}

		// var buf [20]byte
		b := strconv.AppendUint(buf[:0], f.num, 10)

		_, err := fe.Write(b)
		return err
	case tokenTypeF64Num:
		// var buf [23]byte

		n := math.Float64frombits(f.num)
		b := strconv.AppendFloat(buf[:0], n, 'g', -1, 64)

		_, err := fe.Write(b)
		return err
	// case tokenTypeF32Num:
	// 	// var buf [23]byte

	// 	n := math.Float64frombits(f.num)
	// 	b := strconv.AppendFloat(buf[:0], n, 'g', -1, 32)

	// 	_, err := fe.Write(b)
	// 	return err
	case tokenTypeTime:
		// var buf [30]byte

		ts := time.Unix(0, int64(f.num)).UTC()
		b := ts.AppendFormat(buf[:0], time.RFC3339Nano)

		_, err := fe.Write(b)
		return err
	default:
		panic("unknown csv field type")
	}
}

func Raw(v []byte) Field {
	return Field{
		raw:       v,
		tokenType: tokenTypeRaw,
	}
}

func Str(v string) Field {
	return Field{
		str:       v,
		tokenType: tokenTypeStr,
	}
}

// func Int[T ~int | ~int32 | ~int64](v T) Field {
// 	return Field{
// 		num:       uint64(v),
// 		tokenType: tokenTypeINum,
// 	}
// }

func Uint[T ~uint | ~uint32 | ~uint64](v T) Field {
	return Field{
		num:       uint64(v),
		tokenType: tokenTypeUNum,
	}
}

func Float64(v float64) Field {
	if math.IsNaN(v) {
		return emptyField
	}

	return Field{
		num:       math.Float64bits(v),
		tokenType: tokenTypeF64Num,
	}
}

// func Float32(v float32) Field {
// 	return Field{
// 		num:       math.Float64bits(float64(v)),
// 		tokenType: tokenTypeF32Num,
// 	}
// }

func Time(v time.Time) Field {
	return Field{
		num:       uint64(v.UnixNano()),
		tokenType: tokenTypeTime,
	}
}

func (fe *fieldEncoder) WriteFields(fields ...Field) error {
	var writeSep func() error
	writeSep = func() error {
		writeSep = func() error {
			_, err := fe.Write([]byte{','})
			return err
		}

		return nil
	}

	for _, field := range fields {
		if err := writeSep(); err != nil {
			return err
		}

		if err := field.MarshalCSVFieldTo(fe); err != nil {
			return err
		}
	}

	return nil
}

func (fe *fieldEncoder) writeRow(fields ...Field) error {
	if err := fe.WriteFields(fields...); err != nil {
		return err
	}

	_, err := fe.Write([]byte{'\n'})
	return err
}

func newEncoder(w io.Writer) *fieldEncoder {
	return &fieldEncoder{Writer: w}
}

func WriteRow(w io.Writer, fields ...Field) error {
	return newEncoder(w).writeRow(fields...)
}
