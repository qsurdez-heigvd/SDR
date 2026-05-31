package logging

import (
	"chatsapp/internal/utils/ioutils"
	"fmt"
)

// LogLevel describes the level of importance of a log message.
type LogLevel uint8

const (
	// INFO is the lowest logging level. Used for general information messages.
	INFO LogLevel = 1
	// WARN is important information that may indicate a problem.
	WARN LogLevel = 2
	// ERR is the highest logging level. Used for error messages.
	ERR LogLevel = 3
)

// Logger is a struct that logs messages to the standard output and/or a file.
type Logger struct {
	ioStream ioutils.IOStream
	file     *LogFile
	name     string
	logLevel LogLevel
	fileOnly bool
}

// NewLogger constructs and returns a new logger instance.
func NewLogger(ioStream ioutils.IOStream, file *LogFile, name string, fileOnly bool) *Logger {
	return &Logger{
		ioStream: ioStream,
		file:     file,
		name:     name,
		fileOnly: fileOnly,
		logLevel: INFO,
	}
}

// NewStdLogger returns a new instance of a logger that logs to the standard output.
func NewStdLogger(name string) *Logger {
	return NewLogger(ioutils.NewStdStream(), nil, name, false)
}

// WithLogLevel returns a new logger with the same configuration, but with a filter on the log level: only messages of higher or equal level will be logged.
func (l *Logger) WithLogLevel(level LogLevel) *Logger {
	return &Logger{
		ioStream: l.ioStream,
		file:     l.file,
		name:     l.name,
		logLevel: level,
		fileOnly: l.fileOnly,
	}
}

// WithPostfix returns a new logger with the same configuration, but with the given postfix appended to the name.
func (l *Logger) WithPostfix(postfix string) *Logger {
	return &Logger{
		ioStream: l.ioStream,
		file:     l.file,
		name:     fmt.Sprintf("%s|%s", l.name, postfix),
		logLevel: l.logLevel,
		fileOnly: l.fileOnly,
	}
}

// Info logs a message with the INFO level.
func (l *Logger) Info(args ...interface{}) {
	if l.logLevel > INFO {
		return
	}
	s := fmt.Sprintf("[INFO|%s] %s\n", l.name, fmt.Sprint(args...))
	if l.file != nil {
		l.file.Print(s)
	}
	if !l.fileOnly {
		// nl.ioStream.Print(s)
		fmt.Print(s)
	}
}

// Infof logs a formatted message with the INFO level.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.Info(fmt.Sprintf(format, args...))
}

// Warn logs a message with the WARN level.
func (l *Logger) Warn(args ...interface{}) {
	if l.logLevel > WARN {
		return
	}
	s := fmt.Sprintf("[WARN|%s] %s\n", l.name, fmt.Sprint(args...))
	if l.file != nil {
		l.file.Print(s)
	}
	if !l.fileOnly {
		// nl.ioStream.Print(s)
		fmt.Print(s)
	}
}

// Warnf logs a formatted message with the WARN level.
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.Warn(fmt.Sprintf(format, args...))
}

// Error logs a message with the ERR level.
func (l *Logger) Error(args ...interface{}) {
	if l.logLevel > ERR {
		return
	}
	s := fmt.Sprintf("[ERROR|%s] %s\n", l.name, fmt.Sprint(args...))
	if l.file != nil {
		l.file.Print(s)
	}
	if !l.fileOnly {
		// nl.ioStream.Print(s)
		fmt.Print(s)
	}
}

// Errorf logs a formatted message with the ERR level.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Error(fmt.Sprintf(format, args...))
}
