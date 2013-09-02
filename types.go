package monetdb

import (
	"fmt"
	"time"
)

type Time struct {
	Hour, Min, Sec int
}

type Date struct {
	Year  int
	Month time.Month
	Day   int
}

func (t Time) String() string {
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour, t.Min, t.Sec)
}

func (d Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day)
}

func GetTime(t time.Time) Time {
	hour, min, sec := t.Clock()
	return Time{hour, min, sec}
}

func GetDate(t time.Time) Date {
	year, month, day := t.Date()
	return Date{year, month, day}
}
