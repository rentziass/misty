package logger

type (
	Logger interface {
		Info(...interface{})
		Debug(...interface{})
		Warn(...interface{})
		Error(...interface{})
	}

	emptyLogger struct{}
)

var (
	DefaultLogger = &emptyLogger{}
)

func (emptyLogger) Info(...interface{})  {}
func (emptyLogger) Debug(...interface{}) {}
func (emptyLogger) Warn(...interface{})  {}
func (emptyLogger) Error(...interface{}) {}
