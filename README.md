go-monetdb
==========

MonetDB driver for Go.

[![Build Status](https://travis-ci.org/fajran/go-monetdb.png?branch=master)](https://travis-ci.org/fajran/go-monetdb)

## Installation

To install the `monetdb` package to your `$GOPATH`, simply use
the `go` tool. Make sure you have [Git](http://git-scm.com/downloads) installed.

```bash
$ go get github.com/fajran/go-monetdb
```

## Usage

This Go MonetDB driver implements Go's
[`database/sql/driver`](http://golang.org/pkg/database/sql/driver/) interface.
Once you import it, you can use the standard Go database API to access MonetDB.

```go
import (
	"database/sql"
	_ "github.com/fajran/go-monetdb"
)
```

Then use `monetdb` as the driver name and Data Source Name (DSN) as specified
in the next section.

```go
db, err := sql.Open("monetdb", "username:password@hostname:50000/database")
```

## Data Source Name (DSN)

The format of the DSN is the following

```
[username[:password]@]hostname[:port]/database
```

Currently, you can only use a domain name or an IPv4 address for the hostname.
IPv6 address is not yet supported (#2 - feel free to send pull request).

If the `port` is blank, then the default port `50000` will be used.

