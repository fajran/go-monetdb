/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

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
