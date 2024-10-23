package repl

import (
	"github.com/c-bata/go-prompt"
	"io"
)

const flushMaxRetryCount = 3

// PosixWriter is a ConsoleWriter implementation for POSIX environment.
// To control terminal emulator, this outputs VT100 escape sequences.
type PosixWriter struct {
	VT100Writer
	out io.Writer
}

// Flush to flush buffer
func (w *PosixWriter) Flush() error {
	l := len(w.buffer)
	offset := 0
	retry := 0
	for {
		n, err := w.out.Write(w.buffer[offset:])
		if err != nil {
			if retry < flushMaxRetryCount {
				retry++
				continue
			}
			return err
		}
		offset += n
		if offset == l {
			break
		}
	}
	w.buffer = []byte{}
	return nil
}

var _ prompt.ConsoleWriter = &PosixWriter{}

var (
	// NewStandardOutputWriter returns ConsoleWriter object to write to stdout.
	// This generates VT100 escape sequences because almost terminal emulators
	// in POSIX OS built on top of a VT100 specification.
	// Deprecated: Please use NewStdoutWriter
	NewStandardOutputWriter = NewStdoutWriter
)

// NewStdoutWriter returns ConsoleWriter object to write to stdout.
// This generates VT100 escape sequences because almost terminal emulators
// in POSIX OS built on top of a VT100 specification.
func NewStdoutWriter(out io.Writer) prompt.ConsoleWriter {
	return &PosixWriter{
		out: out,
	}
}

// NewStderrWriter returns ConsoleWriter object to write to stderr.
// This generates VT100 escape sequences because almost terminal emulators
// in POSIX OS built on top of a VT100 specification.
func NewStderrWriter(out io.Writer) prompt.ConsoleWriter {
	return &PosixWriter{
		out: out,
	}
}
