package misty

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/rentziass/misty/logger"
)

const (
	OperationOther = iota
	OperationCopy
)

type (
	Table struct {
		Name string

		// Columns is a map of column names and their indices
		Columns map[string]int
	}

	Target struct {
		TableName      string
		Columns        []*TargetColumn
		DeleteRowRules []*DeleteRule
	}

	TargetColumn struct {
		Name  string
		Value func([]byte) []byte
	}

	DeleteRule struct {
		ColumnName   string
		ShouldDelete func([]byte) bool
	}

	Obfuscator struct {
		logger  logger.Logger
		targets []*Target
	}
)

func Obfuscate(r io.Reader, writer io.Writer, targets []*Target) error {
	instance := NewObfuscator(logger.DefaultLogger, targets)
	return instance.Obfuscate(r, writer)
}

func NewObfuscator(logger logger.Logger, targets []*Target) *Obfuscator {
	return &Obfuscator{
		logger:  logger,
		targets: targets,
	}
}

func (o Obfuscator) Obfuscate(r io.Reader, writer io.Writer) error {
	buffer := bufio.NewReader(r)

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
				table = parseCopyFields(string(line))
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
		_, err := writer.Write(line)
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

func parseCopyFields(line string) *Table {
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
