package main

import (
"context"
"log"
)

// Custom logger implementing the schematic-go Logger interface
type SchematicLogger struct {
	level LogLevel
}

type LogLevel int

const (
LogLevelDebug LogLevel = iota
LogLevelInfo
LogLevelWarn
LogLevelError
)

func NewSchematicLogger() *SchematicLogger {
	return &SchematicLogger{
		level: LogLevelInfo,
	}
}

func (l *SchematicLogger) Debug(ctx context.Context, msg string) {
	if l.level <= LogLevelDebug {
		log.Printf("[DEBUG] %s", msg)
	}
}

func (l *SchematicLogger) Info(ctx context.Context, msg string) {
	if l.level <= LogLevelInfo {
		log.Printf("[INFO] %s", msg)
	}
}

func (l *SchematicLogger) Warn(ctx context.Context, msg string) {
	if l.level <= LogLevelWarn {
		log.Printf("[WARN] %s", msg)
	}
}

func (l *SchematicLogger) Error(ctx context.Context, msg string) {
	if l.level <= LogLevelError {
		log.Printf("[ERROR] %s", msg)
	}
}

func (l *SchematicLogger) SetLevel(level LogLevel) {
	l.level = level
}
