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
	"encoding/csv"
	"fmt"
	"github.com/bluesign/tinyAN/storage"
	"github.com/c-bata/go-prompt"
	"github.com/gliderlabs/ssh"
	"github.com/onflow/cadence"
	"github.com/onflow/cadence/ast"
	"github.com/onflow/cadence/common"
	jsoncdc "github.com/onflow/cadence/encoding/json"
	"github.com/onflow/cadence/interpreter"
	"github.com/onflow/cadence/pretty"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/cadence/sema"
	"github.com/onflow/flow-go/fvm"
	"github.com/onflow/flow-go/fvm/environment"
	reusableRuntime "github.com/onflow/flow-go/fvm/runtime"
	fvmStorage "github.com/onflow/flow-go/fvm/storage"
	fvmState "github.com/onflow/flow-go/fvm/storage/state"
	"github.com/onflow/flow-go/fvm/tracing"
	flowgo "github.com/onflow/flow-go/model/flow"
	prettyJSON "github.com/tidwall/pretty"
	"io"
	"strings"
)

type ConsoleREPL struct {
	lineIsContinuation bool
	code               string
	lineNumber         int
	errorPrettyPrinter pretty.ErrorPrettyPrinter
	repl               *REPL
	historyWriter      *csv.Writer
	session            ssh.Session
	out                *stringWriter
	lastPosition       ast.Position
}
type stringWriter struct {
	w io.Writer
}

func (w stringWriter) WriteString(s string) (int, error) {
	return w.w.Write([]byte(s))
}
func (w stringWriter) Write(b []byte) (int, error) {
	return w.w.Write(b)
}

func NewConsoleREPL(store *storage.HeightBasedStorage, session ssh.Session) (*ConsoleREPL, error) {

	sw := &stringWriter{
		w: session,
	}

	consoleREPL := &ConsoleREPL{
		lineNumber:         1,
		errorPrettyPrinter: pretty.NewErrorPrettyPrinter(sw, true),
		session:            session,
		out:                sw,
	}

	var block *flowgo.Header
	var err error

	block, err = store.GetBlockByHeight(store.Latest().LastBlocksHeight() - 2)
	if err != nil {
		return nil, err
	}

	snapshot := store.LedgerSnapshot(block.Height)

	debugger := interpreter.NewDebugger()

	var entropyProvider = &storage.EntropyProviderPerBlockProvider{
		Store: store,
	}

	fvmContext := fvm.NewContext(
		fvm.WithBlockHeader(block),
		fvm.WithBlocks(store),
		fvm.WithEntropyProvider(entropyProvider.AtBlockID(block.ID())),
		fvm.WithCadenceLogging(true),
		fvm.WithAuthorizationChecksEnabled(false),
		fvm.WithSequenceNumberCheckAndIncrementEnabled(false),
		fvm.WithEVMEnabled(true),
		fvm.WithMemoryLimit(2*1024*1024*1024), //2GB
		fvm.WithComputationLimit(10_000),      //100k
		fvm.WithReusableCadenceRuntimePool(
			reusableRuntime.NewReusableCadenceRuntimePool(
				0,
				runtime.Config{
					Debugger:           debugger,
					TracingEnabled:     false,
					AttachmentsEnabled: true,
				},
			),
		),
	)

	blockDatabase := fvmStorage.NewBlockDatabase(snapshot, 0, nil)
	txnState, err := blockDatabase.NewTransaction(0, fvmState.DefaultParameters())
	if err != nil {
		panic(err)
	}
	fvmContext.TxId = flowgo.ZeroID
	fvmContext.TxIndex = 0
	fvmContext.TxBody = flowgo.NewTransactionBody()

	env := environment.NewTransactionEnvironment(
		tracing.NewMockTracerSpan(),
		fvmContext.EnvironmentParams,
		txnState)

	addr, err := flowgo.StringToAddress("7e60df042a9c0868")
	if err != nil {
		fmt.Println(err)
	}
	used, err := env.GetStorageUsed(common.MustBytesToAddress(addr.Bytes()))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("used", used)

	cadenceRepl, err := NewREPL(&runtimeWrapper{
		baseRuntime: env,
		REPL:        consoleREPL.repl,
	})

	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Println("cadenceRepl", cadenceRepl)
	cadenceRepl.OnError = consoleREPL.onError
	cadenceRepl.OnResult = consoleREPL.onResult

	consoleREPL.repl = cadenceRepl
	return consoleREPL, nil
}

func (consoleREPL *ConsoleREPL) onError(err error, location common.Location, codes map[common.Location][]byte) {
	printErr := consoleREPL.errorPrettyPrinter.PrettyPrintError(err, location, codes)
	if printErr != nil {
		panic(printErr)
	}
}

func (consoleREPL *ConsoleREPL) onResult(value interpreter.Value) {
	consoleREPL.out.WriteString(colorizeValue(value) + "\n")
}

func (consoleREPL *ConsoleREPL) handleCommand(command string) {
	parts := strings.SplitN(command, " ", 2)
	for _, command := range commands {
		if command.name != parts[0][1:] {
			continue
		}

		var argument string
		if len(parts) > 1 {
			argument = parts[1]
		}

		command.handler(consoleREPL, argument)
		return
	}

	printError(consoleREPL.out, fmt.Sprintf("Unknown command. %s", replAssistanceMessage))
}

func (consoleREPL *ConsoleREPL) exportVariable(name string) {
	repl := consoleREPL.repl

	global := repl.GetGlobal(name)
	if global == nil {
		printError(consoleREPL.out, fmt.Sprintf("Undefined global: %s", name))
		return
	}

	value, err := repl.ExportValue(global)
	if err != nil {
		printError(consoleREPL.out, fmt.Sprintf("Failed to export global %s: %s", name, err))
		return
	}

	json, err := jsoncdc.Encode(value)
	if err != nil {
		printError(consoleREPL.out, fmt.Sprintf("Failed to encode global %s to JSON: %s", name, err))
		return
	}

	_, _ = consoleREPL.session.Write(prettyJSON.Color(prettyJSON.Pretty(json), nil))
}

func (consoleREPL *ConsoleREPL) showType(expression string) {
	repl := consoleREPL.repl

	oldOnExpressionType := repl.OnExpressionType
	repl.OnExpressionType = func(ty sema.Type) {
		fmt.Fprintf(consoleREPL.out, colorizeResult(string(ty.ID()))+"\n")
	}
	defer func() {
		repl.OnExpressionType = oldOnExpressionType
	}()

	_, err := repl.Accept([]byte(expression+"\n"), false)
	if err == nil {
		consoleREPL.lineNumber++
	}
}

func (consoleREPL *ConsoleREPL) execute(line string) {
	if consoleREPL.code == "" && strings.HasPrefix(line, ".") {
		consoleREPL.handleCommand(line)
		consoleREPL.code = ""
		return
	}

	consoleREPL.code += line + "\n"

	inputIsComplete, err := consoleREPL.repl.Accept([]byte(consoleREPL.code), true)
	if err == nil {
		consoleREPL.lineNumber++

		if !inputIsComplete {
			consoleREPL.lineIsContinuation = true
			return
		}
	}

	err = consoleREPL.appendHistory()
	if err != nil {
		panic(err)
	}

	consoleREPL.lineIsContinuation = false
	consoleREPL.code = ""
}

func (consoleREPL *ConsoleREPL) suggest(d prompt.Document) []prompt.Suggest {
	wordBeforeCursor := d.GetWordBeforeCursor()

	if len(wordBeforeCursor) == 0 {
		return nil
	}

	var suggests []prompt.Suggest

	if wordBeforeCursor[0] == commandPrefix {
		commandLookupPrefix := wordBeforeCursor[1:]

		for _, command := range commands {
			if !strings.HasPrefix(command.name, commandLookupPrefix) {
				continue
			}
			suggests = append(suggests, prompt.Suggest{
				Text:        fmt.Sprintf("%c%s", commandPrefix, command.name),
				Description: command.description,
			})
		}

	} else {
		for _, suggestion := range consoleREPL.repl.Suggestions(wordBeforeCursor) {
			suggests = append(suggests, prompt.Suggest{
				Text:        suggestion.Name,
				Description: suggestion.Description,
			})
		}
	}

	//words := strings.Split(wordBeforeCursor, ".")

	return prompt.FilterHasPrefix(suggests, wordBeforeCursor, false)
}

func (consoleREPL *ConsoleREPL) changeLivePrefix() (string, bool) {
	separator := '>'
	if consoleREPL.lineIsContinuation {
		separator = '.'
	}

	return fmt.Sprintf("%d%c ", consoleREPL.lineNumber, separator), true
}

func (consoleREPL *ConsoleREPL) Run() {

	consoleREPL.printWelcome()

	history, _ := consoleREPL.readHistory()
	err := consoleREPL.openHistoryWriter()
	if err != nil {
		panic(err)
	}

	prompt.New(
		consoleREPL.execute,
		consoleREPL.suggest,
		prompt.OptionLivePrefix(consoleREPL.changeLivePrefix),
		prompt.OptionHistory(history),
		prompt.OptionWriter(NewStandardOutputWriter(consoleREPL.out)),
		prompt.OptionParser(NewStandardInputParser(consoleREPL.session)),
	).Run()
}

func printError(out *stringWriter, message string) {
	out.WriteString(colorizeError(message) + "\n")
}

const commandPrefix = '.'

func (consoleREPL *ConsoleREPL) readHistory() ([]string, error) {

	return []string{}, nil
}

func (consoleREPL *ConsoleREPL) openHistoryWriter() error {

	return nil
}

func (consoleREPL *ConsoleREPL) appendHistory() error {

	return nil
}

const replAssistanceMessage = `Type '.help' for assistance.`

const replHelpMessagePrefix = `
Enter declarations and statements to evaluate them.
Commands are prefixed with a dot. Valid commands are:
`

const replHelpMessageSuffix = `
Press ^C to abort current expression, ^D to exit
`

func (consoleREPL *ConsoleREPL) printHelp() {
	fmt.Fprintln(consoleREPL.out, replHelpMessagePrefix)

	for _, command := range commands {
		fmt.Fprintf(consoleREPL.out,
			"%c%s\t%s\n",
			commandPrefix,
			command.name,
			command.description,
		)
	}

	fmt.Fprintln(consoleREPL.out, replHelpMessageSuffix)
}

type command struct {
	name        string
	description string
	handler     func(repl *ConsoleREPL, argument string)
}

var commands []command

func init() {
	commands = []command{
		{
			name:        "exit",
			description: "Exit the interpreter",
			handler: func(r *ConsoleREPL, _ string) {
				r.session.Close()
			},
		},
		{
			name:        "help",
			description: "Show help",
			handler: func(consoleREPL *ConsoleREPL, _ string) {
				consoleREPL.printHelp()
			},
		},
		{
			name:        "export",
			description: "Export variable",
			handler: func(consoleREPL *ConsoleREPL, argument string) {
				name := strings.TrimSpace(argument)
				if len(name) == 0 {
					printError(consoleREPL.out, "Missing name")
					return
				}
				consoleREPL.exportVariable(name)
			},
		},
		{
			name:        "type",
			description: "Show type of expression",
			handler: func(consoleREPL *ConsoleREPL, argument string) {
				if len(argument) == 0 {
					printError(consoleREPL.out, "Missing expression")
					return
				}

				consoleREPL.showType(argument)
			},
		},
	}
}

func (consoleREPL *ConsoleREPL) printWelcome() {
	fmt.Fprintf(consoleREPL.out, "Welcome to Cadence %s!\n%s\n\n", cadence.Version, replAssistanceMessage)
}
