package misty

type Logger interface {
	Info(...interface{})
	Debug(...interface{})
	Warn(...interface{})
	Error(...interface{})
}

type emptyLogger struct{}

func (emptyLogger) Info(...interface{})  {}
func (emptyLogger) Debug(...interface{}) {}
func (emptyLogger) Warn(...interface{})  {}
func (emptyLogger) Error(...interface{}) {}
