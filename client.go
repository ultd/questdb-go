package questdb

import (
	"bufio"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"

	_ "github.com/lib/pq"
)

// Config is a struct which holds Client's config fields
type Config struct {
	ILPHost           string
	ILPAuthPrivateKey string
	ILPAuthKid        string
	PGConnStr         string
	TLSConfig         *tls.Config
}

// Client struct represents a QuestDB client connection. This encompasses the InfluxDB Line
// protocol net.TCPConn as well as the Postgres wire *sql.DB connection. Methods on this
// client are primarily used to read/write data to QuestDB.
type Client struct {
	config Config
	// ilpConn is the TCP connection which allows Client to write data to QuestDB
	ilpConn net.Conn
	// pgSqlDB is the Postgres SQL DB connection which allows to read/query data from QuestDB
	pgSqlDB *sql.DB
}

// Default func returns a *Client with the default config as specified by QuestDB docs
func Default() *Client {
	return &Client{
		config: Config{
			ILPHost:           "localhost:9009",
			ILPAuthPrivateKey: "",
			ILPAuthKid:        "",
			PGConnStr:         "postgresql://admin:quest@localhost:8812/qdb?sslmode=disable",
		},
	}
}

// New func returns a *Client and an optional error given a Config
func New(config Config) (*Client, error) {
	return &Client{
		config: config,
	}, nil
}

var (
	ErrILPNetDial           = errors.New("could not dial ilp host")
	ErrILPTLSDial           = errors.New("could not dial tls host")
	ErrILPNetTCPAddrResolve = errors.New("could not resolve ilp host address")
	ErrPGOpen               = errors.New("could not open postgres db")
)

// Connect func dials and connects both the Influx line protocol TCP connection as well
// as the underlying sql PG database connection.
func (c *Client) Connect() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", c.config.ILPHost)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrILPNetTCPAddrResolve, err)
	}

	if c.config.TLSConfig != nil {
		conn, err := tls.Dial("tcp", c.config.ILPHost, c.config.TLSConfig)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrILPTLSDial, err)
		}
		c.ilpConn = conn
	} else {
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrILPNetDial, err)
		}
		c.ilpConn = conn
	}

	if c.config.ILPAuthPrivateKey != "" {
		if c.config.ILPAuthKid == "" {
			return fmt.Errorf("cannot authenticate ilp without 'ILPAuthKid' set in config")
		}

		// Parse and create private key
		keyRaw, err := base64.RawURLEncoding.DecodeString(c.config.ILPAuthPrivateKey)
		if err != nil {
			return fmt.Errorf("could not base64 decode ilp private key: %w", err)
		}
		key := new(ecdsa.PrivateKey)
		key.PublicKey.Curve = elliptic.P256()
		key.PublicKey.X, key.PublicKey.Y = key.PublicKey.Curve.ScalarBaseMult(keyRaw)
		key.D = new(big.Int).SetBytes(keyRaw)

		// send key ID

		reader := bufio.NewReader(c.ilpConn)
		_, err = c.ilpConn.Write([]byte(c.config.ILPAuthKid + "\n"))
		if err != nil {
			return fmt.Errorf("could not write to ilp tcp conn: %w", err)
		}

		raw, err := reader.ReadBytes('\n')
		if err != nil {
			return fmt.Errorf("could not read from ilp conn: %w", err)
		}
		// Remove the `\n` is last position
		raw = raw[:len(raw)-1]

		// Hash the challenge with sha256
		hash := crypto.SHA256.New()
		hash.Write(raw)
		hashed := hash.Sum(nil)

		a, b, err := ecdsa.Sign(rand.Reader, key, hashed)
		if err != nil {
			return fmt.Errorf("could not ecdsa sign key: %w", err)
		}
		stdSig := append(a.Bytes(), b.Bytes()...)
		_, err = c.ilpConn.Write([]byte(base64.StdEncoding.EncodeToString(stdSig) + "\n"))
		if err != nil {
			return fmt.Errorf("could not write to ilp tcp conn: %w", err)
		}
	}

	db, err := sql.Open("postgres", c.config.PGConnStr)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPGOpen, err)
	}

	c.pgSqlDB = db

	return nil
}

// Close func closes both the Influx line protocol TCP connection as well as
// the PG sql database connection
func (c *Client) Close() error {
	errs := []error{}
	if err := c.pgSqlDB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("could not close pg sql db: %w", err))
	}
	if err := c.ilpConn.Close(); err != nil {
		errs = append(errs, fmt.Errorf("could not close ilp tcp conn: %w", err))
	}
	errStr := ""
	for i, err := range errs {
		if i > 0 {
			errStr += " "
		}
		errStr += fmt.Sprintf("%d: %s;", i, err)
	}

	if errStr != "" {
		return fmt.Errorf("%s", errStr)
	}

	return nil
}

// WriteMessage func takes a message and writes it to the underlying InfluxDB line protocol
func (c *Client) WriteMessage(message []byte) error {
	_, err := c.ilpConn.Write(message)
	if err != nil {
		return err
	}
	return nil
}

// Write takes a valid struct with qdb tags and writes it to the underlying InfluxDB line protocol
func (c *Client) Write(a interface{}, options ...option) error {
	m, err := NewModel(a)
	if err != nil {
		return err
	}

	if len(options) > 0 {
		for _, opt := range options {
			// check and set all options here
			if opt.tableName != "" {
				m.tableName = opt.tableName
			}
		}
	}

	line := m.MarshalLine()
	_, err = c.ilpConn.Write(line)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) WriteBatch(rows []interface{}, options ...option) error {
	var models []*Model
	for _, row := range rows {
		m, err := NewModel(row)
		if err != nil {
			return err
		}
		if len(options) > 0 {
			for _, opt := range options {
				// check and set all options here
				if opt.tableName != "" {
					m.tableName = opt.tableName
				}
			}
		}
		models = append(models, m)
	}

	var sb strings.Builder
	for _, m := range models {
		sb.Write(m.MarshalLine())
	}
	_, err := c.ilpConn.Write([]byte(sb.String()))
	if err != nil {
		return err
	}
	return nil
}

// DB func returns the underlying *sql.DB struct for DB operations over the Postgres wire protocol
func (c *Client) DB() *sql.DB {
	return c.pgSqlDB
}

// CreateTableIfNotExists func takes a valid 'qdb' tagged struct v and attempts to create the table
// (via the PG wire) in QuestDB and returns an possible error. You can optionally pass a custom table name.
func (c *Client) CreateTableIfNotExists(v interface{}, options ...option) error {
	// make model from v
	model, err := NewModel(v)
	if err != nil {
		return fmt.Errorf("could not make new model: %w", err)
	}

	if len(options) > 0 {
		for _, opt := range options {
			// check and set options here
			if opt.tableName != "" {
				model.tableName = opt.tableName
			}
		}
	}

	// execute create table if not exists statement
	_, err = c.DB().Exec(model.CreateTableIfNotExistStatement())
	if err != nil {
		return fmt.Errorf("could not execute sql statement: %w", err)
	}

	return nil
}
