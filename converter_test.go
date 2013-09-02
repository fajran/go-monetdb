package monetdb

import (
	"database/sql/driver"
	"testing"
	"time"
)

func TestConvertToMonet(t *testing.T) {
	type tc struct {
		v driver.Value
		e string
	}
	var tcs = []tc{
		tc{1, "1"},
		tc{"string", "'string'"},
		tc{"quoted 'string'", "'quoted \\'string\\''"},
		tc{"quoted \"string\"", "'quoted \"string\"'"},
		tc{int8(8), "8"},
		tc{int16(16), "16"},
		tc{int32(32), "32"},
		tc{int64(64), "64"},
		tc{float32(3.2), "3.2"},
		tc{float64(6.4), "6.4"},
		tc{true, "true"},
		tc{false, "false"},
		tc{nil, "NULL"},
		tc{[]byte{1, 2, 3}, "'" + string([]byte{1, 2, 3}) + "'"},
		tc{Time{10, 20, 30}, "'10:20:30'"},
		tc{Date{2001, time.January, 2}, "'2001-01-02'"},
		tc{time.Date(2001, time.January, 2, 10, 20, 30, 0, time.FixedZone("CET", 3600)),
			"'2001-01-02 10:20:30 +0100 CET'"},
	}

	for _, c := range tcs {
		s, err := convertToMonet(c.v)
		if err != nil {
			t.Errorf("Error converting value: %v -> %v", c.v, err)
		} else if s != c.e {
			t.Errorf("Invalid value: %s, expected: %s", s, c.e)
		}
	}
}

func TestConvertToGo(t *testing.T) {
	type tc struct {
		v string
		t string
		e driver.Value
	}
	var tcs = []tc{
		tc{"8", "tinyint", int8(8)},
		tc{"16", "smallint", int16(16)},
		tc{"16", "shortint", int16(16)},
		tc{"32", "int", int32(32)},
		tc{"32", "mediumint", int32(32)},
		tc{"64", "bigint", int64(64)},
		tc{"64", "longint", int64(64)},
		tc{"64", "serial", int64(64)},
		tc{"3.2", "float", float32(3.2)},
		tc{"3.2", "real", float32(3.2)},
		tc{"6.4", "double", float64(6.4)},
		tc{"6.4", "decimal", float64(6.4)},
		tc{"true", "boolean", true},
		tc{"false", "boolean", false},
		tc{"10:20:30", "time", Time{10, 20, 30}},
		tc{"2001-01-02", "date", Date{2001, time.January, 2}},
		tc{"'string'", "char", "string"},
		tc{"'string'", "varchar", "string"},
		tc{"'quoted \"string\"'", "char", "quoted \"string\""},
		tc{"'quoted \\'string\\''", "char", "quoted 'string'"},
		// tc{"'ABC'", "blob", []uint8{0x41, 0x42, 0x43}},
	}

	for _, c := range tcs {
		v, err := convertToGo(c.v, c.t)
		if err != nil {
			t.Errorf("Error converting value: %v (%s) -> %v", c.v, c.t, err)
		} else if v != c.e {
			t.Errorf("Invalid value: %v (%v - %s), expected: %v", v, c.v, c.t, c.e)
		}
	}
}
