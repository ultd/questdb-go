package questdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"

	_ "github.com/lib/pq"
)

type RowScanner interface {
	Scan(dst ...interface{}) error
}

// ScanIntoer is an interface with a single method "ScanInto" which takes a
// RowScanner. It's up to the end user how to Scan into the data using the RowScanner.
type ScanIntoer interface {
	ScanInto(scanner RowScanner) error
}

type NewScanintoer func() ScanIntoer

type Config struct {
	ilpHost   string
	pgConnStr string
}

// Client struct represents a QuestDB client connection. This encompasses the InfluxDB Line
// protocol net.TCPConn as well as the Postgres wire *sql.DB connection. Methods on this
// client are primarily used to read/write data to QuestDB.
type Client struct {
	config  Config
	ilpConn *net.TCPConn
	pgSqlDB *sql.DB
}

// Default
func Default() *Client {
	return &Client{
		config: Config{
			ilpHost:   "localhost:9009",
			pgConnStr: "postgresql://admin:quest@localhost:8812/qdb",
		},
	}
}

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

func (c *Client) WriteRow(row string) error {
	_, err := c.ilpConn.Write([]byte(fmt.Sprintf("%s\n", row)))
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) QueryRow(ctx context.Context, query string, s ScanIntoer) error {
	row := c.pgSqlDB.QueryRowContext(ctx, query)
	if err := row.Err(); err != nil {
		return err
	}

	if err := s.ScanInto(row); err != nil {
		return fmt.Errorf("could not scan: %w", err)
	}

	return nil
}

func (c *Client) QueryRows(ctx context.Context, query string, dest *[]interface{}, new NewScanintoer) error {
	rows, err := c.pgSqlDB.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		thing := new()
		rows.Scan()
		if err := thing.ScanInto(rows); err != nil {
			return fmt.Errorf("could not scan: %w", err)
		}
		*dest = append(*dest, thing)
	}
	if rows.Err(); err != nil {
		return fmt.Errorf("could not prepare next row: %w", err)
	}

	return nil
}
