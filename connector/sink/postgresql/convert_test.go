package postgresql

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  any
	}{
		{name: "nil stays SQL NULL", value: nil, want: nil},
		{name: "int64 passes through", value: int64(-7), want: int64(-7)},
		{name: "uint64 fits int64", value: uint64(math.MaxInt64), want: int64(math.MaxInt64)},
		{name: "uint64 above MaxInt64 keeps exact text", value: uint64(math.MaxInt64) + 1, want: "9223372036854775808"},
		{name: "uint64 max keeps exact text", value: uint64(math.MaxUint64), want: "18446744073709551615"},
		{name: "decimal text passes through", value: "12345.6789", want: "12345.6789"},
		{name: "timestamp text passes through", value: "2024-01-02 03:04:05.000000", want: "2024-01-02 03:04:05.000000"},
		{name: "bytes pass through", value: []byte{0x00, 0xff}, want: []byte{0x00, 0xff}},
		{name: "unknown shapes become text", value: time.Second, want: "1s"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ConvertValue(tc.value)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v (%T) want %#v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}
