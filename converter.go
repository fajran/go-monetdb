package monetdb

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"
	"reflect"
)

const (
	CHAR      = "char"    // (L) character string with length L
	VARCHAR   = "varchar" // (L) string with atmost length L
	CLOB      = "clob"
	BLOB      = "blob"
	DECIMAL   = "decimal"  // (P,S)
	SMALLINT  = "smallint" // 16 bit integer
	INT       = "int"      // 32 bit integer
	BIGINT    = "bigint"   // 64 bit integer
	SERIAL    = "serial"   // special 64 bit integer sequence generator
	REAL      = "real"     // 32 bit floating point
	DOUBLE    = "double"   // 64 bit floating point
	BOOLEAN   = "boolean"
	DATE      = "date"
	TIME      = "time"      // (T) time of day
	TIMESTAMP = "timestamp" // (T) date concatenated with unique time
	INTERVAL  = "interval"  // (Q) a temporal interval

	MONTH_INTERVAL = "month_interval"
	SEC_INTERVAL   = "sec_interval"
	WRD            = "wrd"
	TINYINT        = "tinyint"

	// Not on the website:
	SHORTINT    = "shortint"
	MEDIUMINT   = "mediumint"
	LONGINT     = "longint"
	FLOAT       = "float"
	TIMESTAMPTZ = "timestamptz"

	// full names and aliases, spaces are replaced with underscores
	CHARACTER               = CHAR
	CHARACTER_VARYING       = VARCHAR
	CHARACHTER_LARGE_OBJECT = CLOB
	BINARY_LARGE_OBJECT     = BLOB
	NUMERIC                 = DECIMAL
	DOUBLE_PRECISION        = DOUBLE
)

var timeFormats = []string{
	"2006-01-02",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05 -0700",
	"2006-01-02 15:04:05 -0700 MST",
	"Mon Jan 2 15:04:05 -0700 MST 2006",
	"15:04:05",
}

type toGoConverter func(string) (driver.Value, error)
type toMonetConverter func(driver.Value) (string, error)

func strip(v string) (driver.Value, error) {
	return strings.TrimSpace(v[1 : len(v)-1]), nil
}

func toByteArray(v string) (driver.Value, error) {
	return []byte(v), nil
}

func toDouble(v string) (driver.Value, error) {
	return strconv.ParseFloat(v, 64)
}

func toFloat(v string) (driver.Value, error) {
	var r float32
	i, err := strconv.ParseFloat(v, 32)
	if err != nil {
		r = float32(i)
	}
	return r, err
}

func toInt8(v string) (driver.Value, error) {
	var r int8
	i, err := strconv.ParseInt(v, 10, 8)
	if err == nil {
		r = int8(i)
	}
	return r, err
}

func toInt16(v string) (driver.Value, error) {
	var r int16
	i, err := strconv.ParseInt(v, 10, 16)
	if err == nil {
		r = int16(i)
	}
	return r, err
}

func toInt32(v string) (driver.Value, error) {
	var r int32
	i, err := strconv.ParseInt(v, 10, 32)
	if err == nil {
		r = int32(i)
	}
	return r, err
}

func toInt64(v string) (driver.Value, error) {
	var r int64
	i, err := strconv.ParseInt(v, 10, 64)
	if err == nil {
		r = int64(i)
	}
	return r, err
}

func parseTime(v string) (t time.Time, err error) {
	for _, f := range timeFormats {
		t, err = time.Parse(f, v)
		if err == nil {
			return
		}
	}
	return
}

func toBool(v string) (driver.Value, error) {
	return strconv.ParseBool(v)
}

func toDate(v string) (driver.Value, error) {
	t, err := parseTime(v)
	if err != nil {
		return nil, err
	}
	year, month, day := t.Date()
	return Date{year, month, day}, nil
}

func toTime(v string) (driver.Value, error) {
	t, err := parseTime(v)
	if err != nil {
		return nil, err
	}
	hour, min, sec := t.Clock()
	return Time{hour, min, sec}, nil
}
func toTimestamp(v string) (driver.Value, error) {
	return parseTime(v)
}
func toTimestampTz(v string) (driver.Value, error) {
	return parseTime(v)
}

var toGoMappers = map[string]toGoConverter{
	CHAR:           strip,
	VARCHAR:        strip,
	CLOB:           strip,
	BLOB:           toByteArray,
	DECIMAL:        toDouble,
	SMALLINT:       toInt16,
	INT:            toInt32,
	WRD:            toInt32,
	BIGINT:         toInt64,
	SERIAL:         toInt32,
	REAL:           toFloat,
	DOUBLE:         toDouble,
	BOOLEAN:        toBool,
	DATE:           toDate,
	TIME:           toTime,
	TIMESTAMP:      toTimestamp,
	TIMESTAMPTZ:    toTimestampTz,
	INTERVAL:       strip,
	MONTH_INTERVAL: strip,
	SEC_INTERVAL:   strip,
	TINYINT:        toInt8,
	SHORTINT:       toInt16,
	MEDIUMINT:      toInt32,
	LONGINT:        toInt64,
	FLOAT:          toFloat,
}

func toString(v driver.Value) (string, error) {
	return fmt.Sprintf("%v", v), nil
}

func toQuotedString(v driver.Value) (string, error) {
	s := fmt.Sprintf("%v", v)
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "'", "\\'", -1)
	return fmt.Sprintf("'%v'", s), nil
}

func toNull(v driver.Value) (string, error) {
	return "NULL", nil
}

func toByteString(v driver.Value) (string, error) {
	switch val := v.(type) {
	case []uint8:
		return toQuotedString(string(val))
	default:
		return "", fmt.Errorf("unsupported type")
	}
}

func toDateTimeString(v driver.Value) (string, error) {
	switch val := v.(type) {
	case Time:
		return toQuotedString(fmt.Sprintf("%02d:%02d:%02d", val.Hour, val.Min, val.Sec))
	case Date:
		return toQuotedString(fmt.Sprintf("%04d-%02d-%02d", val.Year, val.Month, val.Day))
	default:
		return "", fmt.Errorf("unsupported type")
	}
}

var toMonetMappers = map[string]toMonetConverter{
	"int": toString,
	"int8": toString,
	"int16": toString,
	"int32": toString,
	"int64": toString,
	"float": toString,
	"float32": toString,
	"float64": toString,
	"bool": toString,
	"string": toQuotedString,
	"nil": toNull,
	"[]uint8": toByteString,
	"time.Time": toQuotedString,
	"monetdb.Time": toDateTimeString,
	"monetdb.Date": toDateTimeString,
}

func convertToGo(value, dataType string) (driver.Value, error) {
	if mapper, ok := toGoMappers[dataType]; ok {
		value := strings.TrimSpace(value)
		return mapper(value)
	}
	return nil, fmt.Errorf("type not supported: %s", dataType)
}

func convertToMonet(value driver.Value) (string, error) {
	t := reflect.TypeOf(value)
	n := "nil"
	if t != nil {
		n = t.String()
	}

	if mapper, ok := toMonetMappers[n]; ok {
		return mapper(value)
	}
	return "", fmt.Errorf("type not supported: %v", t)
}
