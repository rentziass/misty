package misty

import (
	"bufio"
	"bytes"
	"io"
)

// TableIndexEntry describes the position of the COPY statement
// of a table within the dump.
type TableIndexEntry struct {
	// Name is the name of the table
	Name string
	// StartingLine is the line at which the COPY
	// statement begins. This is to help humans debugging this
	StartingLine int
	// StartingAt is the byte position at which the copy statement begins.
	// By using this you can point that position directly when you want to
	// read this table COPY statement in particular
	StartingAt int64
	// EndingAt is the byte position at which the COPY statement ends,
	// so you know exactly how many bytes you need to read
	EndingAt int64
}

// TablesIndex is a slice of *TableIndexEntry
type TablesIndex []*TableIndexEntry

// BuildTablesIndex scans the whole dump file looking for COPY
// statements, and builds an index that is then returned. This kind of
// scan is required if you want to run the obfuscation concurrently.
func BuildTablesIndex(r io.Reader) TablesIndex {
	buffer := bufio.NewReader(r)
	currentPosition := 0
	operation := OperationOther
	var table *Table

	var tableMaps TablesIndex

	for currentLine := 1; ; currentLine++ {
		line, readErr := buffer.ReadBytes('\n')
		if readErr != nil && readErr == io.EOF {
			break
		}

		switch operation {
		case OperationOther:
			if bytes.HasPrefix(line, []byte("COPY ")) {
				table = parseCopyStatementFields(string(line))
				operation = OperationCopy
				m := &TableIndexEntry{
					Name:         table.Name,
					StartingAt:   int64(currentPosition),
					StartingLine: currentLine,
				}
				tableMaps = append(tableMaps, m)
			}
		case OperationCopy:
			if bytes.Equal(line, []byte("\\.\n")) {
				operation = OperationOther
				m := tableMaps[len(tableMaps)-1]
				m.EndingAt = int64(currentPosition + len(line) - 1)
				tableMaps[len(tableMaps)-1] = m
			}
		}

		currentPosition += len(line)
	}

	return tableMaps
}
