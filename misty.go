package misty

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

const (
	OperationOther = iota
	OperationCopy
	OperationIgnore
)

type Obfuscator struct {
	Errors []error

	dumpFile *os.File
	reader   io.Reader
	writer   io.Writer

	targets   []*Target
	tableMaps []*TableIndexEntry

	tmpDir string

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

func NewObfuscator(f *os.File, r io.Reader, w io.Writer, targets []*Target, options ...Option) *Obfuscator {
	obfuscator := &Obfuscator{
		dumpFile:    f,
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
		return o.parallelObfuscate()
	}

	return o.obfuscateAll()
}

func (o *Obfuscator) parallelObfuscate() error {
	o.tableMaps = BuildTablesIndex(o.reader)
	tmpDir, err := ioutil.TempDir("", "obfuscator")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	o.tmpDir = tmpDir

	var wg sync.WaitGroup
	for i, t := range o.tableMaps {
		wg.Add(1)
		tableNumber := i
		tableMap := t
		go func() {
			o.obfuscateTable(tableNumber, tmpDir, tableMap)
			wg.Done()
		}()
	}

	wg.Wait()

	buffer := bufio.NewReader(o.dumpFile)

	operation := OperationCopy

	var table *Table
	currentTable := 0

	for {
		fmt.Println("inside")
		line, readErr := buffer.ReadBytes('\n')
		if readErr != nil && readErr == io.EOF {
			break
		}

		switch operation {
		case OperationCopy:
			if bytes.HasPrefix(line, []byte("COPY ")) {
				table = parseCopyStatementFields(string(line))
				for _, t := range o.targets {
					if t.TableName == table.Name {
						operation = OperationIgnore
						o.copyObfuscatedTable(currentTable)
						currentTable++
						continue
					}
				}
			}
			currentTable++
			fmt.Println("about to write")
			_, err := o.writer.Write(line)
			if err != nil {
				return err
			}
		case OperationIgnore:
			if bytes.Equal(line, []byte("\\.\n")) {
				operation = OperationCopy
			}
		}
	}

	return nil
}

func (o *Obfuscator) copyObfuscatedTable(tableNumber int) error {
	tableFile, err := os.Open(fmt.Sprintf("%s/%v", o.tmpDir, tableNumber))
	if err != nil {
		return err
	}

	buffer := bufio.NewReader(tableFile)
	for {
		line, readErr := buffer.ReadBytes('\n')
		if readErr != nil && readErr == io.EOF {
			break
		}

		_, err = o.writer.Write(line)
		if err != nil {
			return err
		}
	}
	return nil
}

// Read and obfuscates a single table, outputting the result
// in a temporary file inside the given folder
func (o *Obfuscator) obfuscateTable(tableNumber int, tmpDir string, tableMap *TableIndexEntry) {
	target := o.targetForTable(tableMap.Name)
	// if no target is provided we don't wanna move forward
	if target == nil {
		return
	}

	filename := fmt.Sprintf("%s/table_%v", tmpDir, tableNumber)
	f, err := os.Create(filename)
	if err != nil {
		o.Errors = append(o.Errors, err)
	}
	defer f.Close()

	operation := OperationOther

	tableBytes := make([]byte, tableMap.EndingAt-tableMap.StartingAt)
	_, err = o.dumpFile.ReadAt(tableBytes, tableMap.StartingAt)
	if err != nil {
		o.Errors = append(o.Errors, err)
	}

	buffer := bytes.NewBuffer(tableBytes)
	if err != nil {
		o.Errors = append(o.Errors, err)
	}

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
						o.logger.Info("starting table: ", table.Name)
					}
				}

				if targetForTable == nil {
					o.logger.Info(fmt.Sprintf("Ignoring table %s\n", table.Name))
				}
			}
		case OperationCopy:
			if bytes.Equal(line, []byte("\\.\n")) {
				operation = OperationOther
				o.logger.Info("done processing table: ", table.Name)
			} else {
				hasNewlineSuffix := bytes.HasSuffix(line, []byte("\n"))
				if hasNewlineSuffix {
					line = line[:len(line)-1]
				}
				err := processDataLine(targetForTable, table, &line)
				if err != nil {
					o.Errors = append(o.Errors, fmt.Errorf("error while processing line %v: %v", currentLine, err))
				} else if hasNewlineSuffix && len(line) > 0 {
					line = append(line, '\n')
				}
			}
		}
		_, err := f.Write(line)
		if err != nil {
			o.Errors = append(o.Errors, err)
		}
	}
}

func (o *Obfuscator) targetForTable(table string) *Target {
	for _, t := range o.targets {
		if t.TableName == table {
			return t
		}
	}
	return nil
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
