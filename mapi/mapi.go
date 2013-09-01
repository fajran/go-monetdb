package mapi

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
	MAX_PACKAGE_LENGTH = (1024 * 8) - 2

	MSG_PROMPT   = ""
	MSG_INFO     = "#"
	MSG_ERROR    = "!"
	MSG_Q        = "&"
	MSG_QTABLE   = "&1"
	MSG_QUPDATE  = "&2"
	MSG_QSCHEMA  = "&3"
	MSG_QTRANS   = "&4"
	MSG_QPREPARE = "&5"
	MSG_QBLOCK   = "&6"
	MSG_HEADER   = "%"
	MSG_TUPLE    = "["
	MSG_REDIRECT = "^"
	MSG_OK       = "=OK"

	STATE_INIT  = 0
	STATE_READY = 1
)

var (
	MSG_MORE = string([]byte{1, 2, 10})
)

type Conn struct {
	Hostname string
	Port     int
	Username string
	Password string
	Database string
	Language string

	State int

	conn *net.TCPConn
}

func New(hostname string, port int, username, password, database, language string) *Conn {
	return &Conn{
		Hostname: hostname,
		Port:     port,
		Username: username,
		Password: password,
		Database: database,
		Language: language,

		State: STATE_INIT,
	}
}

func (c *Conn) Disconnect() {
	c.State = STATE_INIT
	c.conn.Close()
	c.conn = nil
}

func (c *Conn) Cmd(operation string) (string, error) {
	if c.State != STATE_READY {
		return "", fmt.Errorf("not connected")
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

	} else if strings.HasPrefix(resp, MSG_OK) {
		return strings.TrimSpace(resp[3:]), nil

	} else if resp == MSG_MORE {
		// tell server it isn't going to get more
		return c.Cmd("")

	} else if strings.HasPrefix(resp, MSG_Q) || strings.HasPrefix(resp, MSG_HEADER) || strings.HasPrefix(resp, MSG_TUPLE) {
		return resp, nil

	} else if strings.HasPrefix(resp, MSG_ERROR) {
		return "", fmt.Errorf("Error: %s", resp[1:])

	} else {
		return "", fmt.Errorf("unknown state: %s", resp)
	}
}

func (c *Conn) Connect() error {
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

func (c *Conn) login() error {
	return c.tryLogin(0)
}

func (c *Conn) tryLogin(iteration int) error {
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

	} else if prompt == MSG_OK {
		// pass

	} else if strings.HasPrefix(prompt, MSG_INFO) {
		// TODO log info

	} else if strings.HasPrefix(prompt, MSG_ERROR) {
		// TODO log error
		return fmt.Errorf("%s", prompt[1:])

	} else if strings.HasPrefix(prompt, MSG_REDIRECT) {
		t := strings.Split(prompt, " ")
		r := strings.Split(t[0][1:], ":")

		if r[1] == "merovingian" {
			// restart auth
			if iteration <= 10 {
				c.tryLogin(iteration + 1)
			} else {
				return fmt.Errorf("maximal number of redirects reached (10)")
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
			return fmt.Errorf("unknown redirect: %s", prompt)
		}
	} else {
		return fmt.Errorf("unknown state: %s", prompt)
	}

	c.State = STATE_READY

	return nil
}

func (c *Conn) challengeResponse(challenge []byte) (string, error) {
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

func (c *Conn) getBlock() ([]byte, error) {
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

func (c *Conn) getBytes(count int) ([]byte, error) {
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

func (c *Conn) putBlock(b []byte) error {
	pos := 0
	last := 0
	for last != 1 {
		end := pos + MAX_PACKAGE_LENGTH
		if end > len(b) {
			end = len(b)
		}
		data := b[pos:end]
		length := len(data)
		if length < MAX_PACKAGE_LENGTH {
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
