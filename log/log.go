package log

import (
	fastlogger "github.com/995933447/fastlog/logger"
	"github.com/995933447/fastlog/logger/writer"
)

type Logger interface {
	Debugf(format string, args ...any)
	Debug(args any)
	Infof(format string, args ...any)
	Info(args any)
	Importantf(format string, args ...any)
	Important(args any)
	Warnf(format string, args ...any)
	Warn(args any)
	Errorf(format string, args ...any)
	Error(args any)
	Fatalf(format string, args ...any)
	Fatal(args any)
	Panicf(format string, args ...any)
	Panic(args any)
}

var logger Logger = NewStdoutLogger()

func SetLogger(l Logger) {
	logger = l
}

func Debugf(format string, args ...any) {
	logger.Debugf(format, args...)
}

func Debug(args any) {
	logger.Debug(args)
}

func Infof(format string, args ...any) {
	logger.Infof(format, args...)
}

func Info(args any) {
	logger.Info(args)
}

func Importantf(format string, args ...any) {
	logger.Importantf(format, args...)
}

func Important(args any) {
	logger.Important(args)
}

func Warnf(format string, args ...any) {
	logger.Warnf(format, args...)
}

func Warn(args any) {
	logger.Warn(args)
}

func Errorf(format string, args ...any) {
	logger.Errorf(format, args...)
}

func Error(args any) {
	logger.Error(args)
}

func Fatalf(format string, args ...any) {
	logger.Fatalf(format, args...)
}

func Fatal(args any) {
	logger.Fatal(args)
}

func Panicf(format string, args ...any) {
	logger.Panicf(format, args...)
}

func Panic(args any) {
	logger.Panic(args)
}

var _ Logger = (*StdoutLogger)(nil)

func NewStdoutLogger() *StdoutLogger {
	return &StdoutLogger{
		logger: fastlogger.NewLogger(writer.NewStdoutWriter(fastlogger.LevelDebug, "easymicro", 6)),
	}
}

type StdoutLogger struct {
	logger *fastlogger.Logger
}

func (l *StdoutLogger) Importantf(format string, args ...any) {
	l.logger.Importantf(format, args...)
}

func (l *StdoutLogger) Important(args any) {
	l.logger.Important(args)
}

func (l *StdoutLogger) Debugf(format string, args ...any) {
	l.logger.Debugf(format, args...)
}

func (l *StdoutLogger) Debug(args any) {
	l.logger.Debug(args)
}

func (l *StdoutLogger) Infof(format string, args ...any) {
	l.logger.Infof(format, args...)
}

func (l *StdoutLogger) Info(args any) {
	l.logger.Info(args)
}

func (l *StdoutLogger) Warnf(format string, args ...any) {
	l.logger.Warnf(format, args...)
}

func (l *StdoutLogger) Warn(args any) {
	l.logger.Warn(args)
}

func (l *StdoutLogger) Errorf(format string, args ...any) {
	l.logger.Errorf(format, args...)
}

func (l *StdoutLogger) Error(args any) {
	l.logger.Error(args)
}

func (l *StdoutLogger) Fatalf(format string, args ...any) {
	l.logger.Fatalf(format, args...)
}

func (l *StdoutLogger) Fatal(args any) {
	l.logger.Fatal(args)
}

func (l *StdoutLogger) Panicf(format string, args ...any) {
	l.logger.Panicf(format, args...)
}

func (l *StdoutLogger) Panic(args any) {
	l.logger.Panic(args)
}
