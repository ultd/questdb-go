package questdb

import (
	"fmt"
	"time"
)

// Line struct represents an InfluxDB line protocol message
type Line struct {
	table     string
	symbols   map[string]string
	columns   map[string]string
	timestamp time.Time
}

// NewLine returns a *Line
func NewLine(table string, symbols map[string]string, columns map[string]string, timestamp time.Time) *Line {
	return &Line{
		table:     table,
		symbols:   symbols,
		columns:   columns,
		timestamp: timestamp,
	}
}

func (l *Line) buildSymbols() string {
	if len(l.symbols) == 0 {
		return ""
	}

	out := ""
	n := 0
	for key, val := range l.symbols {
		out += fmt.Sprintf("%s=%s", key, val)
		if n != len(l.symbols)-1 {
			out += ","
		}
		n++
	}

	return out
}

func (l *Line) buildColumns() string {
	if len(l.columns) == 0 {
		return ""
	}

	out := ""
	n := 0
	for key, val := range l.columns {
		out += fmt.Sprintf("%s=%s", key, val)
		if n != len(l.columns)-1 {
			out += ","
		}
		n++
	}

	return out
}

func (l *Line) buildTimestamp() string {
	return fmt.Sprintf("%d", l.timestamp.UnixNano())
}

// String func returns the Line as InfluxDB line protocol message string
func (l *Line) String() string {
	symbolsString := l.buildSymbols()
	columnsString := l.buildColumns()
	timestampString := l.buildTimestamp()

	outString := l.table

	if symbolsString != "" {
		outString += fmt.Sprintf(",%s", symbolsString)
	}

	if columnsString != "" {
		outString += fmt.Sprintf(" %s", columnsString)
	}

	if timestampString != "" {
		outString += fmt.Sprintf(" %s", timestampString)
	}

	outString += "\n"

	return outString
}
