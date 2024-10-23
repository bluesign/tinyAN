package repl

import (
	"fmt"
	prompt "github.com/c-bata/go-prompt"
	"github.com/gliderlabs/ssh"
	"io"
	"log"
)

const maxReadBytes = 1024

// PosixParser is a ConsoleParser implementation for POSIX environment.
type PosixParser struct {
	reader  io.Reader
	session ssh.Session
	w       int
	h       int
}

// Setup should be called before starting input
func (t *PosixParser) Setup() error {

	return nil
}

// TearDown should be called after stopping input
func (t *PosixParser) TearDown() error {

	return nil
}

// Read returns byte array.
func (t *PosixParser) Read() ([]byte, error) {
	buf := make([]byte, maxReadBytes)
	n, err := t.reader.Read(buf)
	if err != nil {
		return []byte{}, err
	}
	return buf[:n], nil
}

// GetWinSize returns WinSize object to represent width and height of terminal.
func (t *PosixParser) GetWinSize() *prompt.WinSize {
	t.session.Pty()
	return &prompt.WinSize{
		Row: uint16(t.w),
		Col: uint16(t.h),
	}
}

func sizeWatcher(p *PosixParser) {
	pty, changes, ok := p.session.Pty()

	if !ok {
		log.Println("Failed to get pty")
		p.w = 80
		p.h = 20
		return
	}

	fmt.Println("pty", pty)

	p.w = 80 //pty.Window.Width
	p.h = 40 //pty.Window.Height

	for {
		select {
		case <-changes:
			p.w = pty.Window.Width
			p.h = pty.Window.Height

		case <-p.session.Context().Done():
			return
		}
	}
}

var _ prompt.ConsoleParser = &PosixParser{}

// NewStandardInputParser returns ConsoleParser object to read from stdin.
func NewStandardInputParser(session ssh.Session) *PosixParser {

	p := &PosixParser{
		session: session,
		reader:  session,
		w:       80,
		h:       40,
	}
	go sizeWatcher(p)
	return p
}
