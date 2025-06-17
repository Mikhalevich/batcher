package logger

type Logger interface {
	Info(args ...interface{})
	Error(args ...interface{})

	WithError(err error) Logger
	WithField(key string, value interface{}) Logger
}
