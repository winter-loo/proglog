package errs

import (
	"fmt"
	"runtime"
)

// SourceError wraps an error with file and line context.
type SourceError struct {
	Err  error
	File string
	Line int
}

// Wrap captures the current stack frame and wraps the error.
func Wrap(err error) error {
	if err == nil {
		return nil
	}
	// 1 skips the stack frame of this Wrap function itself.
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	}
	return &SourceError{
		Err:  err,
		File: file,
		Line: line,
	}
}

// Error implements the error interface.
func (e *SourceError) Error() string {
	// Format: /path/to/file.go:32: original error message
	return fmt.Sprintf("%s:%d: %v", e.File, e.Line, e.Err)
}

// Unwrap allows standard errors.Is/As checks to work on the underlying error.
func (e *SourceError) Unwrap() error {
	return e.Err
}
