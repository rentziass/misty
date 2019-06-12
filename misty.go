package misty

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	OperationOther = iota
	OperationCopy
)

type Obfuscator struct {
	Errors []error

	reader io.Reader
	writer io.Writer

	targets   []*Target
	tableMaps []*TableIndexEntry

	maxRoutines    int
	tableSemaphore chan bool

	logger Logger
}

type Option func(*Obfuscator)

func WithMaxRoutines(n int) func(*Obfuscator) {
	return func(obfuscator *Obfuscator) {
		obfuscator.maxRoutines = n
	}
}

func WithLogger(logger Logger) func(*Obfuscator) {
	return func(obfuscator *Obfuscator) {
		obfuscator.logger = logger
	}
}

func NewObfuscator(r io.Reader, w io.Writer, targets []*Target, options ...Option) *Obfuscator {
	obfuscator := &Obfuscator{
		reader:      r,
		writer:      w,
		targets:     targets,
		maxRoutines: 1,
		logger:      &emptyLogger{},
	}

	for _, option := range options {
		option(obfuscator)
	}

	obfuscator.tableSemaphore = make(chan bool, obfuscator.maxRoutines)

	return obfuscator
}

func (o *Obfuscator) Run() error {
	if o.maxRoutines > 1 {
		o.parallelObfuscate()
		if len(o.Errors) > 1 {
			return errors.New("there was an error running the obfuscator, check Obfuscator.Errors for more details")
		}
		return nil
	}

	return o.obfuscateAll()
}

func (o *Obfuscator) parallelObfuscate() {
	o.tableMaps = BuildTablesIndex(o.reader)
	for i, t := range o.tableMaps {
		go o.obfuscateTable(i, t)
	}
}

// Read and obfuscates a single table, outputting the result
// in a temporary file
func (o *Obfuscator) obfuscateTable(tableNumber int, tableMap *TableIndexEntry) {

}

type Table struct {
	Name string

	// Columns is a map of column names and their indices
	Columns map[string]int
}

type Target struct {
	TableName      string
	Columns        []*TargetColumn
	DeleteRowRules []*DeleteRule
}

type TargetColumn struct {
	Name  string
	Value func([]byte) []byte
}

type DeleteRule struct {
	ColumnName   string
	ShouldDelete func([]byte) bool
}

func (o *Obfuscator) obfuscateAll() error {
	buffer := bufio.NewReader(o.reader)

	operation := OperationOther

	var table *Table
	var targetForTable *Target

	for currentLine := 1; ; currentLine++ {
		line, readErr := buffer.ReadBytes('\n')
		if readErr != nil && readErr == io.EOF {
			break
		}

		switch operation {
		case OperationOther:
			if bytes.HasPrefix(line, []byte("COPY ")) {
				table = parseCopyStatementFields(string(line))
				targetForTable = nil
				for _, t := range o.targets {
					if t.TableName == table.Name {
						targetForTable = t
						operation = OperationCopy
						o.logger.Info("Beginning to work on table: ", table.Name)
					}
				}

				if targetForTable == nil {
					o.logger.Info(fmt.Sprintf("Ignoring table %s\n", table.Name))
				}
			}
		case OperationCopy:
			if bytes.Equal(line, []byte("\\.\n")) {
				operation = OperationOther
				o.logger.Info("Done.")
			} else {
				hasNewlineSuffix := bytes.HasSuffix(line, []byte("\n"))
				if hasNewlineSuffix {
					line = line[:len(line)-1]
				}
				err := processDataLine(targetForTable, table, &line)
				if err != nil {
					return fmt.Errorf("error while processing line %v: %v", currentLine, err)
				} else if hasNewlineSuffix && len(line) > 0 {
					line = append(line, '\n')
				}
			}
		}
		_, err := o.writer.Write(line)
		if err != nil {
			return err
		}
	}
	return nil
}

func processDataLine(target *Target, table *Table, line *[]byte) error {
	fields := bytes.Split(*line, []byte("\t"))
	if len(fields) != len(table.Columns) {
		return errors.New("invalid number of arguments")
	}

	for _, deleteRule := range target.DeleteRowRules {
		columnIndex, columnPresent := table.Columns[deleteRule.ColumnName]
		if !columnPresent {
			return errors.New(fmt.Sprintf("could not find column %s for table %s", deleteRule.ColumnName, table.Name))
		}

		if deleteRule.ShouldDelete(fields[columnIndex]) {
			*line = []byte{}
			return nil
		}
	}

	for _, targetColumn := range target.Columns {
		columnIndex, columnPresent := table.Columns[targetColumn.Name]
		if !columnPresent {
			return errors.New(fmt.Sprintf("could not find column %s for table %s", targetColumn.Name, table.Name))
		}

		fields[columnIndex] = targetColumn.Value(fields[columnIndex])
	}

	*line = bytes.Join(fields, []byte("\t"))
	return nil
}

func parseCopyStatementFields(line string) *Table {
	delimiters := " \n'\"(),;"
	fields := strings.FieldsFunc(line, func(r rune) bool {
		return strings.ContainsRune(delimiters, r)
	})
	if len(fields) < 4 {
		panic("invalid copy statement")
	}

	columns := map[string]int{}
	for i, c := range fields[2 : len(fields)-2] {
		columns[c] = i
	}

	return &Table{
		Name:    fields[1],
		Columns: columns,
	}
}
