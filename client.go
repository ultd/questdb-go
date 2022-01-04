package questdb

import (
	"database/sql"
	"errors"
	"fmt"
	"net"

	_ "github.com/lib/pq"
)

// Config is a struct which holds Client's config fields
type Config struct {
	ILPHost   string
	PGConnStr string
}

// Client struct represents a QuestDB client connection. This encompasses the InfluxDB Line
// protocol net.TCPConn as well as the Postgres wire *sql.DB connection. Methods on this
// client are primarily used to read/write data to QuestDB.
type Client struct {
	config Config
	// ilpConn is the TCP connection which allows Client to write data to QuestDB
	ilpConn *net.TCPConn
	// pgSqlDB is the Postgres SQL DB connection which allows to read/query data from QuestDB
	pgSqlDB *sql.DB
}

// Default func returns a *Client with the default config as specified by QuestDB docs
func Default() *Client {
	return &Client{
		config: Config{
			ILPHost:   "localhost:9009",
			PGConnStr: "postgresql://admin:quest@localhost:8812/qdb?sslmode=disable",
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
	ErrILPNetTCPAddrResolve = errors.New("could not resolve ilp host address")
	ErrPGOpen               = errors.New("could not open postgres db")
)

// Connect func dials and connects both the Influx line protocol TCP connection as well
// as the underlying sql pg database connection.
func (c *Client) Connect() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", c.config.ILPHost)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrILPNetTCPAddrResolve, err)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrILPNetDial, err)
	}

	c.ilpConn = conn

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
func (c *Client) Write(a interface{}) error {
	m, err := NewModel(a)
	if err != nil {
		return err
	}
	_, err = c.ilpConn.Write(m.MarshalLine())
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
// (via the PG wire) in QuestDB and returns an possible error
func (c *Client) CreateTableIfNotExists(v interface{}) error {
	// make model from v
	model, err := NewModel(v)
	if err != nil {
		return fmt.Errorf("could not make new model: %w", err)
	}

	// execute create table if not exists statement
	_, err = c.DB().Exec(model.CreateTableIfNotExistStatement())
	if err != nil {
		return fmt.Errorf("could not execute sql statement: %w", err)
	}

	return nil
}
