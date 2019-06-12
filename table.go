package misty

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
