package questdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"

	_ "github.com/lib/pq"
)

// Config is a struct which holds Client's config fields
type Config struct {
	ilpHost   string
	pgConnStr string
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
			ilpHost:   "localhost:9009",
			pgConnStr: "postgresql://admin:quest@localhost:8812/qdb?sslmode=disable",
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
	tcpAddr, err := net.ResolveTCPAddr("tcp4", c.config.ilpHost)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrILPNetTCPAddrResolve, err)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrILPNetDial, err)
	}

	c.ilpConn = conn

	db, err := sql.Open("postgres", c.config.pgConnStr)
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

// Write func takes a message and writes it to the underlying InfluxDB line protocol
func (c *Client) Write(message string) error {
	_, err := c.ilpConn.Write([]byte(message))
	if err != nil {
		return err
	}
	return nil
}

// QueryRow func takes a context and a query statement and returns a *sql.Row after
// executing the query to the underlying Postgres Wire protocol sql database connection
func (c *Client) QueryRow(ctx context.Context, query string) *sql.Row {
	row := c.pgSqlDB.QueryRowContext(ctx, query)
	return row
}

// QueryRows func takes a context and a query statement and returns a *sql.Rows and possible error
// after executing the query to the underlying Postgres Wire protocol sql database connection
func (c *Client) QueryRows(ctx context.Context, query string) (*sql.Rows, error) {
	rows, err := c.pgSqlDB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
