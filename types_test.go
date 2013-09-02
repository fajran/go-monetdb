/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"testing"
	"time"
)

func TestTimeToTime(t *testing.T) {
	hour := 1
	minute := 2
	second := 3
	year := 1970
	month := time.January
	day := 1

	v := Time{hour, minute, second}
	time := v.Time()

	if time.Hour() != hour {
		t.Errorf("Invalid hour: %d, expected: %d", time.Hour(), hour)
	}
	if time.Minute() != minute {
		t.Errorf("Invalid minute: %d, expected: %d", time.Minute(), minute)
	}
	if time.Second() != second {
		t.Errorf("Invalid second: %d, expected: %d", time.Second(), second)
	}

	if time.Year() != year {
		t.Errorf("Invalid year: %d, expected: %d", time.Year(), year)
	}
	if time.Month() != month {
		t.Errorf("Invalid month: %v, expected: %v", time.Month(), month)
	}
	if time.Day() != day {
		t.Errorf("Invalid day: %d, expected: %d", time.Day(), day)
	}
}

func TestTimeFromTime(t *testing.T) {
	hour := 1
	minute := 2
	second := 3
	year := 1970
	month := time.January
	day := 1

	time := time.Date(year, month, day, hour, minute, second, 0, time.UTC)
	v := GetTime(time)

	if v.Hour != hour {
		t.Errorf("Invalid hour: %d, expected: %d", v.Hour, hour)
	}
	if v.Min != minute {
		t.Errorf("Invalid minute: %d, expected: %d", v.Min, minute)
	}
	if v.Sec != second {
		t.Errorf("Invalid second: %d, expected: %d", v.Sec, second)
	}
}

func TestDateToTime(t *testing.T) {
	hour := 0
	minute := 0
	second := 0
	year := 2009
	month := time.October
	day := 17

	v := Date{year, month, day}
	time := v.Time()

	if time.Hour() != hour {
		t.Errorf("Invalid hour: %d, expected: %d", time.Hour(), hour)
	}
	if time.Minute() != minute {
		t.Errorf("Invalid minute: %d, expected: %d", time.Minute(), minute)
	}
	if time.Second() != second {
		t.Errorf("Invalid second: %d, expected: %d", time.Second(), second)
	}

	if time.Year() != year {
		t.Errorf("Invalid year: %d, expected: %d", time.Year(), year)
	}
	if time.Month() != month {
		t.Errorf("Invalid month: %v, expected: %v", time.Month(), month)
	}
	if time.Day() != day {
		t.Errorf("Invalid day: %d, expected: %d", time.Day(), day)
	}
}

func TestDateFromTime(t *testing.T) {
	hour := 1
	minute := 2
	second := 3
	year := 2009
	month := time.October
	day := 17

	time := time.Date(year, month, day, hour, minute, second, 0, time.UTC)
	v := GetDate(time)

	if v.Year != year {
		t.Errorf("Invalid year: %d, expected: %d", v.Year, year)
	}
	if v.Month != month {
		t.Errorf("Invalid month: %d, expected: %d", v.Month, month)
	}
	if v.Day != day {
		t.Errorf("Invalid day: %d, expected: %d", v.Day, day)
	}
}
