/*
 * Cadence - The resource-oriented smart contract programming language
 *
 * Copyright Flow Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package repl

import (
	"fmt"
	"github.com/gliderlabs/ssh"
	"github.com/logrusorgru/aurora/v4"
	"github.com/onflow/cadence/ast"
	"github.com/onflow/cadence/common"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/c-bata/go-prompt"

	"github.com/onflow/cadence/interpreter"
)

const commandShortHelp = "h"
const commandLongHelp = "help"
const commandShortContinue = "c"
const commandLongContinue = "continue"
const commandShortNext = "n"
const commandLongNext = "next"
const commandLongExit = "exit"
const commandShortShow = "s"
const commandLongShow = "show"
const commandShortWhere = "w"
const commandLongWhere = "where"
const commandLongBreak = "break"
const commandShortBreak = "b"

var debuggerCommandSuggestions = []prompt.Suggest{
	{Text: commandLongContinue, Description: "Continue"},
	{Text: commandLongNext, Description: "Next / step"},
	{Text: commandLongWhere, Description: "Location info"},
	{Text: commandLongShow, Description: "Show variable(s)"},
	{Text: commandLongBreak, Description: "Set Breakpoint and Go"},
	{Text: commandLongExit, Description: "Exit"},
	{Text: commandLongHelp, Description: "Help"},
}

type InteractiveDebugger struct {
	debugger *interpreter.Debugger
	stop     interpreter.Stop
	output   io.Writer
	session  ssh.Session
	Exit     bool
	codes    map[common.Location][]byte
}

func NewInteractiveDebugger(debugger *interpreter.Debugger, stop interpreter.Stop, session ssh.Session, output io.Writer, codes map[common.Location][]byte) *InteractiveDebugger {

	d := &InteractiveDebugger{
		debugger: debugger,
		stop:     stop,
		output:   output,
		session:  session,
		Exit:     false,
		codes:    codes,
	}

	d.Where()
	d.ShowCode(stop.Interpreter.Location, stop.Statement)
	return d
}

func (d *InteractiveDebugger) Continue() {
	d.debugger.Continue()
}

func (d *InteractiveDebugger) ShowCode(location common.Location, statement ast.Statement) {

	codes := string(d.codes[location])
	codes = codes + "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
	precodes := codes[:statement.StartPosition().Offset]
	coloredCodes := colorizeCode(codes[statement.StartPosition().Offset : statement.EndPosition(nil).Offset+1])
	postcodes := codes[statement.EndPosition(nil).Offset+1:]

	codes = precodes + coloredCodes + postcodes

	screen, _, _ := d.session.Pty()
	height := int(screen.Window.Height - 4)

	codeLines := strings.Split(codes, "\n")
	var startLine = statement.StartPosition().Line - height/2

	if startLine < 0 {
		startLine = 0
	}

	endLine := startLine + height

	if endLine > len(codeLines)-1 {
		endLine = len(codeLines) - 1
	}

	if endLine-startLine > height {
		startLine = endLine - height
	}

	for i, line := range codeLines {
		if i >= startLine && i <= endLine {
			lineNumber := aurora.Colorize(fmt.Sprintf("%d\t", i+1), aurora.WhiteFg|aurora.BrightFg|aurora.BoldFm).String()

			fmt.Fprintf(d.output, "%s\t%s\n", lineNumber, line)
		}
	}
}

func (d *InteractiveDebugger) Break(args []string) {
	if len(args) != 1 {
		fmt.Fprintln(d.output, colorizeError("error: 'break' command requires exactly one argument"))
		return
	}
	line, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		fmt.Fprintln(d.output, colorizeError(fmt.Sprintf("error: invalid line number '%s'", args[0])))
		return
	}

	fmt.Println("Breakpoint set at line", line)
	d.debugger.AddBreakpoint(d.stop.Interpreter.Location, uint(line))
	d.debugger.Continue()

	for {
		select {
		case d.stop = <-d.debugger.Stops():
			fmt.Fprintln(d.output, "\033[H\033[2J")
			d.Where()
			d.ShowCode(d.stop.Interpreter.Location, d.stop.Statement)
			return
		case <-time.After(1 * time.Second):
			d.Exit = true
			return
		}
	}
}

func (d *InteractiveDebugger) Next() {
	d.debugger.RequestPause()
	d.debugger.Continue()

	for {
		select {
		case d.stop = <-d.debugger.Stops():
			fmt.Fprintln(d.output, "\033[H\033[2J")

			d.Where()
			//send clear screen

			d.ShowCode(d.stop.Interpreter.Location, d.stop.Statement)
			return
		case <-time.After(1 * time.Second):
			d.Exit = true
			return
		}
	}

}

// Show shows the values for the variables with the given names.
// If no names are given, lists all non-base variables
func (d *InteractiveDebugger) Show(names []string) {
	inter := d.stop.Interpreter
	current := d.debugger.CurrentActivation(inter)
	switch len(names) {
	case 0:
		for name := range current.FunctionValues() { //nolint:maprange
			fmt.Fprintln(d.output, name)
		}

	case 1:
		name := names[0]
		variable := current.Find(name)
		if variable == nil {
			fmt.Fprintln(d.output, colorizeError(fmt.Sprintf("error: variable '%s' is not in scope", name)))
			return
		}

		fmt.Fprintln(d.output, colorizeValue(variable.GetValue(inter)))

	default:
		for _, name := range names {
			variable := current.Find(name)
			if variable == nil {
				continue
			}

			fmt.Fprintf(
				d.output,
				"%s = %s\n",
				name,
				colorizeValue(variable.GetValue(inter)),
			)
		}
	}
}

func (d *InteractiveDebugger) Run() {

	executor := func(in string) {
		in = strings.TrimSpace(in)

		parts := strings.Split(in, " ")

		command, arguments := parts[0], parts[1:]

		switch command {
		case "":
			d.Next()
		case commandShortContinue, commandLongContinue:
			d.Continue()
		case commandShortNext, commandLongNext:
			d.Next()
		case commandShortBreak, commandLongBreak:
			d.Break(arguments)
		case commandShortShow, commandLongShow:
			d.Show(arguments)
		case commandShortWhere, commandLongWhere:
			d.Where()
		case commandShortHelp, commandLongHelp:
			d.Help()
		case commandLongExit:
			return
		default:
			message := fmt.Sprintf("error: '%s' is not a valid command.\n", in)
			fmt.Fprintln(d.output, colorizeError(message))
		}
	}

	suggest := func(d prompt.Document) []prompt.Suggest {
		wordBeforeCursor := d.GetWordBeforeCursor()
		if len(wordBeforeCursor) == 0 {
			return nil
		}

		return prompt.FilterHasPrefix(debuggerCommandSuggestions, wordBeforeCursor, true)
	}

	exitChecker := func(in string, breakline bool) bool {
		switch in {
		case commandShortContinue, commandLongContinue:
			return breakline
		}
		if d.Exit {
			return true
		}
		return false
	}

	prompt.New(
		executor,
		suggest,
		prompt.OptionPrefix("(cdb) "),
		prompt.OptionSetExitCheckerOnInput(exitChecker),
		prompt.OptionWriter(NewStandardOutputWriter(d.output)),
		prompt.OptionParser(NewStandardInputParser(d.session)),
	).Run()
}

func (d *InteractiveDebugger) Help() {
	w := tabwriter.NewWriter(d.output, 0, 0, 1, ' ', 0)
	for _, suggestion := range debuggerCommandSuggestions {
		_, _ = fmt.Fprintf(w,
			"%s\t\t%s\n",
			suggestion.Text,
			suggestion.Description,
		)
	}
	_ = w.Flush()
}

func (d *InteractiveDebugger) Where() {
	fmt.Fprintf(
		d.output,
		"%s @ %d\n",
		d.stop.Interpreter.Location,
		d.stop.Statement.StartPosition().Line,
	)
}
