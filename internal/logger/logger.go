package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.SugaredLogger
	once         sync.Once
)

// LogLevel represents a logging level
type LogLevel string

const (
	// LevelDebug for detailed troubleshooting
	LevelDebug LogLevel = "debug"
	// LevelInfo for general operational entries
	LevelInfo LogLevel = "info"
	// LevelWarn for non-critical issues
	LevelWarn LogLevel = "warn"
	// LevelError for errors that should be addressed
	LevelError LogLevel = "error"
	// LevelFatal for critical errors that require the application to stop
	LevelFatal LogLevel = "fatal"
)

// Init initializes the global logger
func Init(level LogLevel) {
	once.Do(func() {
		var zapLevel zapcore.Level
		switch level {
		case LevelDebug:
			zapLevel = zapcore.DebugLevel
		case LevelInfo:
			zapLevel = zapcore.InfoLevel
		case LevelWarn:
			zapLevel = zapcore.WarnLevel
		case LevelError:
			zapLevel = zapcore.ErrorLevel
		case LevelFatal:
			zapLevel = zapcore.FatalLevel
		default:
			zapLevel = zapcore.InfoLevel
		}

		encoderConfig := zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		}

		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			zapcore.AddSync(os.Stdout),
			zapLevel,
		)

		logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
		globalLogger = logger.Sugar()
	})
}

// Logger returns the global logger instance
func Logger() *zap.SugaredLogger {
	if globalLogger == nil {
		Init(LevelInfo)
	}
	return globalLogger
}

// Debug logs a debug message with structured key-value pairs
func Debug(msg string, keysAndValues ...any) {
	Logger().Debugw(msg, keysAndValues...)
}

// Info logs an info message with structured key-value pairs
func Info(msg string, keysAndValues ...any) {
	Logger().Infow(msg, keysAndValues...)
}

// Warn logs a warning message with structured key-value pairs
func Warn(msg string, keysAndValues ...any) {
	Logger().Warnw(msg, keysAndValues...)
}

// Error logs an error message with structured key-value pairs
func Error(msg string, keysAndValues ...any) {
	Logger().Errorw(msg, keysAndValues...)
}

// Fatal logs a fatal message with structured key-value pairs and then exits
func Fatal(msg string, keysAndValues ...any) {
	Logger().Fatalw(msg, keysAndValues...)
}

// With returns a logger with the specified key-value pairs
func With(keysAndValues ...any) *zap.SugaredLogger {
	return Logger().With(keysAndValues...)
}
