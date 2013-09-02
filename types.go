/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"fmt"
	"time"
)

// Time represents MonetDB's Time datatype.
type Time struct {
	Hour, Min, Sec int
}

// Time represents MonetDB's Date datatype.
type Date struct {
	Year  int
	Month time.Month
	Day   int
}

// String returns a string representation of a Time
// in the form "HH:YY:MM".
func (t Time) String() string {
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour, t.Min, t.Sec)
}

// Time converts to time.Time. The date is set to January 1, 1970.
func (t Time) Time() time.Time {
	return time.Date(1970, time.January, 1, t.Hour, t.Min, t.Sec, 0, time.UTC)
}

// String returns a string representation of a Date
// in the form "YYYY-MM-DD"
func (d Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day)
}

// Time converts to time.Time. The time is set to 00:00:00.
func (d Date) Time() time.Time {
	return time.Date(d.Year, d.Month, d.Day, 0, 0, 0, 0, time.UTC)
}

// GetTime takes the clock part of a time.Time and put it in a Time
func GetTime(t time.Time) Time {
	hour, min, sec := t.Clock()
	return Time{hour, min, sec}
}

// GetDate takes the date part of a time.Time and put it in a Date
func GetDate(t time.Time) Date {
	year, month, day := t.Date()
	return Date{year, month, day}
}
