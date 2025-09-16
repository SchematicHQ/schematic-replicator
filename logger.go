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

func (l *SchematicLogger) Debug(ctx context.Context, msg string, args ...any) {
	if l.level <= LogLevelDebug {
		if len(args) > 0 {
			log.Printf("[DEBUG] "+msg, args...)
		} else {
			log.Printf("[DEBUG] %s", msg)
		}
	}
}

func (l *SchematicLogger) Info(ctx context.Context, msg string, args ...any) {
	if l.level <= LogLevelInfo {
		if len(args) > 0 {
			log.Printf("[INFO] "+msg, args...)
		} else {
			log.Printf("[INFO] %s", msg)
		}
	}
}

func (l *SchematicLogger) Warn(ctx context.Context, msg string, args ...any) {
	if l.level <= LogLevelWarn {
		if len(args) > 0 {
			log.Printf("[WARN] "+msg, args...)
		} else {
			log.Printf("[WARN] %s", msg)
		}
	}
}

func (l *SchematicLogger) Error(ctx context.Context, msg string, args ...any) {
	if l.level <= LogLevelError {
		if len(args) > 0 {
			log.Printf("[ERROR] "+msg, args...)
		} else {
			log.Printf("[ERROR] %s", msg)
		}
	}
}

func (l *SchematicLogger) SetLevel(level LogLevel) {
	l.level = level
}
