// Code generated by canoto. DO NOT EDIT.
// versions:
// 	canoto v0.10.0
// source: epoch_test.go

package simplex_test

import (
	"io"
	"sync/atomic"
	"unicode/utf8"

	"github.com/StephenButtolph/canoto"
)

// Ensure that unused imports do not error
var (
	_ atomic.Int64

	_ = io.ErrUnexpectedEOF
	_ = utf8.ValidString
)

const (
	canoto__testQC__qc__tag = "\x0a" // canoto.Tag(1, canoto.Len)
)

type canotoData_testQC struct {
	size int
}

// MakeCanoto creates a new empty value.
func (*testQC) MakeCanoto() *testQC {
	return new(testQC)
}

// UnmarshalCanoto unmarshals a Canoto-encoded byte slice into the struct.
//
// OneOf fields are cached during the unmarshaling process.
//
// The struct is not cleared before unmarshaling, any fields not present in the
// bytes will retain their previous values. If a OneOf field was previously
// cached as being set, attempting to unmarshal that OneOf again will return
// canoto.ErrDuplicateOneOf.
func (c *testQC) UnmarshalCanoto(bytes []byte) error {
	r := canoto.Reader{
		B: bytes,
	}
	return c.UnmarshalCanotoFrom(r)
}

// UnmarshalCanotoFrom populates the struct from a canoto.Reader. Most users
// should just use UnmarshalCanoto.
//
// OneOf fields are cached during the unmarshaling process.
//
// The struct is not cleared before unmarshaling, any fields not present in the
// bytes will retain their previous values. If a OneOf field was previously
// cached as being set, attempting to unmarshal that OneOf again will return
// canoto.ErrDuplicateOneOf.
//
// This function enables configuration of reader options.
func (c *testQC) UnmarshalCanotoFrom(r canoto.Reader) error {
	var minField uint32
	for canoto.HasNext(&r) {
		field, wireType, err := canoto.ReadTag(&r)
		if err != nil {
			return err
		}
		if field < minField {
			return canoto.ErrInvalidFieldOrder
		}

		switch field {
		case 1:
			if wireType != canoto.Len {
				return canoto.ErrUnexpectedWireType
			}

			originalUnsafe := r.Unsafe
			r.Unsafe = true
			var msgBytes []byte
			err := canoto.ReadBytes(&r, &msgBytes)
			r.Unsafe = originalUnsafe
			if err != nil {
				return err
			}

			remainingBytes := r.B
			count, err := canoto.CountBytes(remainingBytes, canoto__testQC__qc__tag)
			if err != nil {
				return err
			}

			c.qc = canoto.MakeSlice(c.qc, 1+count)
			if len(msgBytes) != 0 {
				r.B = msgBytes
				err = (&c.qc[0]).UnmarshalCanotoFrom(r)
				r.B = remainingBytes
				if err != nil {
					return err
				}
			}

			for i := range count {
				r.B = r.B[len(canoto__testQC__qc__tag):]
				r.Unsafe = true
				err := canoto.ReadBytes(&r, &msgBytes)
				r.Unsafe = originalUnsafe
				if err != nil {
					return err
				}

				if len(msgBytes) != 0 {
					remainingBytes := r.B
					r.B = msgBytes
					err = (&c.qc[1+i]).UnmarshalCanotoFrom(r)
					r.B = remainingBytes
					if err != nil {
						return err
					}
				}
			}
		default:
			return canoto.ErrUnknownField
		}

		minField = field + 1
	}
	return nil
}

// ValidCanoto validates that the struct can be correctly marshaled into the
// Canoto format.
//
// Specifically, ValidCanoto ensures:
// 1. All OneOfs are specified at most once.
// 2. All strings are valid utf-8.
// 3. All custom fields are ValidCanoto.
func (c *testQC) ValidCanoto() bool {
	if c == nil {
		return true
	}
	for i := range c.qc {
		if !(&c.qc[i]).ValidCanoto() {
			return false
		}
	}
	return true
}

// CalculateCanotoCache populates size and OneOf caches based on the current
// values in the struct.
//
// It is not safe to call this function concurrently.
func (c *testQC) CalculateCanotoCache() {
	if c == nil {
		return
	}
	c.canotoData.size = 0
	for i := range c.qc {
		(&c.qc[i]).CalculateCanotoCache()
		fieldSize := (&c.qc[i]).CachedCanotoSize()
		c.canotoData.size += len(canoto__testQC__qc__tag) + canoto.SizeInt(int64(fieldSize)) + fieldSize
	}
}

// CachedCanotoSize returns the previously calculated size of the Canoto
// representation from CalculateCanotoCache.
//
// If CalculateCanotoCache has not yet been called, it will return 0.
//
// If the struct has been modified since the last call to CalculateCanotoCache,
// the returned size may be incorrect.
func (c *testQC) CachedCanotoSize() int {
	if c == nil {
		return 0
	}
	return c.canotoData.size
}

// MarshalCanoto returns the Canoto representation of this struct.
//
// It is assumed that this struct is ValidCanoto.
//
// It is not safe to call this function concurrently.
func (c *testQC) MarshalCanoto() []byte {
	c.CalculateCanotoCache()
	w := canoto.Writer{
		B: make([]byte, 0, c.CachedCanotoSize()),
	}
	w = c.MarshalCanotoInto(w)
	return w.B
}

// MarshalCanotoInto writes the struct into a canoto.Writer and returns the
// resulting canoto.Writer. Most users should just use MarshalCanoto.
//
// It is assumed that CalculateCanotoCache has been called since the last
// modification to this struct.
//
// It is assumed that this struct is ValidCanoto.
//
// It is not safe to call this function concurrently.
func (c *testQC) MarshalCanotoInto(w canoto.Writer) canoto.Writer {
	if c == nil {
		return w
	}
	for i := range c.qc {
		canoto.Append(&w, canoto__testQC__qc__tag)
		canoto.AppendInt(&w, int64((&c.qc[i]).CachedCanotoSize()))
		w = (&c.qc[i]).MarshalCanotoInto(w)
	}
	return w
}
