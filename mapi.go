/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"bytes"
	"crypto"
	_ "crypto/md5"
	_ "crypto/sha1"
	_ "crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"net"
	"strconv"
	"strings"
)

const (
	mapi_MAX_PACKAGE_LENGTH = (1024 * 8) - 2

	mapi_MSG_PROMPT   = ""
	mapi_MSG_INFO     = "#"
	mapi_MSG_ERROR    = "!"
	mapi_MSG_Q        = "&"
	mapi_MSG_QTABLE   = "&1"
	mapi_MSG_QUPDATE  = "&2"
	mapi_MSG_QSCHEMA  = "&3"
	mapi_MSG_QTRANS   = "&4"
	mapi_MSG_QPREPARE = "&5"
	mapi_MSG_QBLOCK   = "&6"
	mapi_MSG_HEADER   = "%"
	mapi_MSG_TUPLE    = "["
	mapi_MSG_REDIRECT = "^"
	mapi_MSG_OK       = "=OK"

	mapi_STATE_INIT  = 0
	mapi_STATE_READY = 1
)

var (
	mapi_MSG_MORE = string([]byte{1, 2, 10})
)

type MapiConn struct {
	Hostname string
	Port     int
	Username string
	Password string
	Database string
	Language string

	State int

	conn *net.TCPConn
}

func NewMapi(hostname string, port int, username, password, database, language string) *MapiConn {
	return &MapiConn{
		Hostname: hostname,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
		Language: language,

		State: mapi_STATE_INIT,
	}
}

func (c *MapiConn) Disconnect() {
	c.State = mapi_STATE_INIT
	c.conn.Close()
	c.conn = nil
}

func (c *MapiConn) Cmd(operation string) (string, error) {
	if c.State != mapi_STATE_READY {
		return "", fmt.Errorf("Database not connected")
	}

	if err := c.putBlock([]byte(operation)); err != nil {
		return "", err
	}

	r, err := c.getBlock()
	if err != nil {
		return "", err
	}

	resp := string(r)
	if len(resp) == 0 {
		return "", nil

	} else if strings.HasPrefix(resp, mapi_MSG_OK) {
		return strings.TrimSpace(resp[3:]), nil

	} else if resp == mapi_MSG_MORE {
		// tell server it isn't going to get more
		return c.Cmd("")

	} else if strings.HasPrefix(resp, mapi_MSG_Q) || strings.HasPrefix(resp, mapi_MSG_HEADER) || strings.HasPrefix(resp, mapi_MSG_TUPLE) {
		return resp, nil

	} else if strings.HasPrefix(resp, mapi_MSG_ERROR) {
		return "", fmt.Errorf("Operational error: %s", resp[1:])

	} else {
		return "", fmt.Errorf("Unknown state: %s", resp)
	}
}

func (c *MapiConn) Connect() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	addr := fmt.Sprintf("%s:%d", c.Hostname, c.Port)
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return err
	}

	conn.SetKeepAlive(false)
	conn.SetNoDelay(true)
	c.conn = conn

	err = c.login()
	if err != nil {
		return err
	}

	return nil
}

func (c *MapiConn) login() error {
	return c.tryLogin(0)
}

func (c *MapiConn) tryLogin(iteration int) error {
	challenge, err := c.getBlock()
	if err != nil {
		return err
	}

	response, err := c.challengeResponse(challenge)
	if err != nil {
		return err
	}

	c.putBlock([]byte(response))

	bprompt, err := c.getBlock()
	if err != nil {
		return nil
	}

	prompt := strings.TrimSpace(string(bprompt))
	if len(prompt) == 0 {
		// Empty response, server is happy

	} else if prompt == mapi_MSG_OK {
		// pass

	} else if strings.HasPrefix(prompt, mapi_MSG_INFO) {
		// TODO log info

	} else if strings.HasPrefix(prompt, mapi_MSG_ERROR) {
		// TODO log error
		return fmt.Errorf("Database error: %s", prompt[1:])

	} else if strings.HasPrefix(prompt, mapi_MSG_REDIRECT) {
		t := strings.Split(prompt, " ")
		r := strings.Split(t[0][1:], ":")

		if r[1] == "merovingian" {
			// restart auth
			if iteration <= 10 {
				c.tryLogin(iteration + 1)
			} else {
				return fmt.Errorf("Maximal number of redirects reached (10)")
			}

		} else if r[1] == "monetdb" {
			c.Hostname = r[2][2:]
			t = strings.Split(r[3], "/")
			port, _ := strconv.ParseInt(t[0], 10, 32)
			c.Port = int(port)
			c.Database = t[1]
			c.conn.Close()
			c.Connect()

		} else {
			return fmt.Errorf("Unknown redirect: %s", prompt)
		}
	} else {
		return fmt.Errorf("Unknown state: %s", prompt)
	}

	c.State = mapi_STATE_READY

	return nil
}

func (c *MapiConn) challengeResponse(challenge []byte) (string, error) {
	t := strings.Split(string(challenge), ":")
	salt := t[0]
	protocol := t[2]
	hashes := t[3]
	algo := t[5]

	if protocol != "9" {
		return "", fmt.Errorf("We only speak protocol v9")
	}

	var h hash.Hash
	if algo == "SHA512" {
		h = crypto.SHA512.New()
	} else {
		// TODO support more algorithm
		return "", fmt.Errorf("Unsupported algorithm: %s", algo)
	}
	io.WriteString(h, c.Password)
	p := fmt.Sprintf("%x", h.Sum(nil))

	shashes := "," + hashes + ","
	var pwhash string
	if strings.Contains(shashes, ",SHA1,") {
		h = crypto.SHA1.New()
		io.WriteString(h, p)
		io.WriteString(h, salt)
		pwhash = fmt.Sprintf("{SHA1}%x", h.Sum(nil))

	} else if strings.Contains(shashes, ",MD5,") {
		h = crypto.MD5.New()
		io.WriteString(h, p)
		io.WriteString(h, salt)
		pwhash = fmt.Sprintf("{MD5}%x", h.Sum(nil))

	} else {
		return "", fmt.Errorf("Unsupported hash algorithm required for login %s", hashes)
	}

	r := fmt.Sprintf("BIG:%s:%s:%s:%s:", c.Username, pwhash, c.Language, c.Database)
	return r, nil
}

func (c *MapiConn) getBlock() ([]byte, error) {
	r := new(bytes.Buffer)

	last := 0
	for last != 1 {
		flag, err := c.getBytes(2)
		if err != nil {
			return nil, err
		}

		var unpacked uint16
		buf := bytes.NewBuffer(flag)
		err = binary.Read(buf, binary.LittleEndian, &unpacked)
		if err != nil {
			return nil, err
		}

		length := unpacked >> 1
		last = int(unpacked & 1)

		d, err := c.getBytes(int(length))
		if err != nil {
			return nil, err
		}

		r.Write(d)
	}

	return r.Bytes(), nil
}

func (c *MapiConn) getBytes(count int) ([]byte, error) {
	r := make([]byte, count)
	b := make([]byte, count)

	read := 0
	for read < count {
		n, err := c.conn.Read(b)
		if err != nil {
			return nil, err
		}
		copy(r[read:], b[:n])
		read += n
	}

	return r, nil
}

func (c *MapiConn) putBlock(b []byte) error {
	pos := 0
	last := 0
	for last != 1 {
		end := pos + mapi_MAX_PACKAGE_LENGTH
		if end > len(b) {
			end = len(b)
		}
		data := b[pos:end]
		length := len(data)
		if length < mapi_MAX_PACKAGE_LENGTH {
			last = 1
		}

		var packed uint16
		packed = uint16((length << 1) + last)
		flag := new(bytes.Buffer)
		binary.Write(flag, binary.LittleEndian, packed)

		if _, err := c.conn.Write(flag.Bytes()); err != nil {
			return err
		}
		if _, err := c.conn.Write(data); err != nil {
			return err
		}

		pos += length
	}

	return nil
}
