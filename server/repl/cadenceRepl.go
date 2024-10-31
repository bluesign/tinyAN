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
	"bytes"
	"fmt"
	"github.com/gliderlabs/ssh"
	"github.com/onflow/flow-go/fvm/evm"
	"github.com/onflow/flow-go/fvm/evm/debug"
	fvmState "github.com/onflow/flow-go/fvm/storage/state"
	"github.com/onflow/flow-go/fvm/tracing"
	"github.com/onflow/go-ethereum/common/math"
	"io"
	"reflect"
	goRuntime "runtime"
	"sort"
	"unsafe"

	"github.com/bluesign/tinyAN/storage"
	"github.com/onflow/cadence"
	"github.com/onflow/cadence/activations"
	"github.com/onflow/cadence/ast"
	"github.com/onflow/cadence/common"
	"github.com/onflow/cadence/errors"
	"github.com/onflow/cadence/interpreter"
	"github.com/onflow/cadence/parser"
	"github.com/onflow/cadence/parser/lexer"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/cadence/sema"
	"github.com/onflow/cadence/stdlib"
	"github.com/onflow/flow-go/fvm"
	"github.com/onflow/flow-go/fvm/environment"
	fvmStorage "github.com/onflow/flow-go/fvm/storage"
	"github.com/onflow/flow-go/fvm/storage/derived"

	flowgo "github.com/onflow/flow-go/model/flow"
	"github.com/rs/zerolog"
)

type REPL struct {
	logger                 zerolog.Logger
	session                ssh.Session
	cadenceRuntime         runtime.Runtime
	codesAndPrograms       runtime.CodesAndPrograms
	fvmEnvironment         environment.Environment
	inter                  *interpreter.Interpreter
	interpreterEnvironment runtime.Environment
	checker                *sema.Checker
	storageProvider        *storage.HeightBasedStorage
	codes                  map[common.Location][]byte
	debugger               *interpreter.Debugger
	OnError                func(err error, location runtime.Location, codes map[runtime.Location][]byte)
	OnExpressionType       func(sema.Type)
	OnResult               func(interpreter.Value)
	parserConfig           parser.Config
	output                 io.Writer
}

func NewREPL(storageProvider *storage.HeightBasedStorage, session ssh.Session, output io.Writer) (*REPL, error) {

	logger := zerolog.Nop()
	var lastBlock *flowgo.Header
	var err error

	user := session.User()
	blockheight, ok := math.ParseUint64(user)
	if ok {
		lastBlock, err = storageProvider.GetBlockByHeight(blockheight)
		if err != nil {
			logger.Err(err).Msgf("cannot get block by height %v", blockheight)
			blockheight = 0
		} else {
			blockheight = lastBlock.Height
		}
	}

	if blockheight == 0 {
		lastBlock, err = storageProvider.GetLatestBlock()
		if err != nil {
			logger.Err(err).Msgf("cannot get last block %v", err)
		}
		blockheight = lastBlock.Height
	}

	fmt.Println("blockheight", blockheight)

	repl := &REPL{
		logger:          logger,
		storageProvider: storageProvider,
		parserConfig:    parser.Config{},
		output:          output,
		session:         session,
	}

	err = repl.StartAtHeight(lastBlock.Height, nil)
	if err != nil {
		return nil, err
	}

	return repl, nil

}

func (r *REPL) StartAtHeight(height uint64, body *flowgo.TransactionBody) error {
	snap := r.storageProvider.LedgerSnapshot(height)
	debugger := interpreter.NewDebugger()

	blockHeader, err := r.storageProvider.GetBlockByHeight(height)
	if err != nil {
		r.logger.Err(err).Msgf("cannot get block by height")
		return err
	}

	derivedChainData, err := derived.NewDerivedChainData(10)
	if err != nil {
		r.logger.Err(err).Msgf("cannot create derived data cache")
	}

	entropyPerBlock := storage.EntropyProviderPerBlockProvider{
		Store: r.storageProvider,
	}

	fvmOptions := []fvm.Option{
		fvm.WithChain(flowgo.Mainnet.Chain()),
		fvm.WithBlockHeader(blockHeader),
		fvm.WithBlocks(r.storageProvider),
		fvm.WithComputationLimit(100_000),           //100k
		fvm.WithMemoryLimit(2 * 1024 * 1024 * 1024), //2GB
		fvm.WithEVMEnabled(true),
		fvm.WithDerivedBlockData(
			derivedChainData.NewDerivedBlockDataForScript(blockHeader.ID()),
		),
		fvm.WithEntropyProvider(entropyPerBlock.AtBlockID(blockHeader.ID())),
	}

	vmCtx := fvm.NewContext(fvmOptions...)

	var fvmEnvironment environment.Environment
	if body != nil {
		vmCtx.TxId = body.ID()
		vmCtx.TxIndex = 0
		vmCtx.TxBody = body

		blockDatabase := fvmStorage.NewBlockDatabase(snap, 0, vmCtx.DerivedBlockData)
		txnState, err := blockDatabase.NewTransaction(0, fvmState.DefaultParameters())
		if err != nil {
			return err
		}
		fvmEnvironment = environment.NewTransactionEnvironment(
			tracing.NewMockTracerSpan(),
			vmCtx.EnvironmentParams,
			txnState)
	} else {
		fvmEnvironment = environment.NewScriptEnvironmentFromStorageSnapshot(
			vmCtx.EnvironmentParams,
			snap,
		)
	}
	codes := runtime.NewCodesAndPrograms()
	codesRef := &codes
	field := reflect.ValueOf(codesRef).Elem().FieldByName("codes")
	codesInner := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface().(map[common.Location][]byte)
	fmt.Println("codesInner", codesInner)
	cadenceStorage := runtime.NewStorage(fvmEnvironment, fvmEnvironment)

	interpreterEnvironment := runtime.NewScriptInterpreterEnvironment(runtime.Config{
		AttachmentsEnabled: true,
		Debugger:           debugger,
	})

	err = evm.SetupEnvironment(flowgo.Mainnet, fvmEnvironment, interpreterEnvironment, debug.NopTracer)
	if err != nil {
		fmt.Println("Error in setup environment", err)
		return err
	}

	wrappedInterface := &RuntimeWrapper{
		BaseRuntime: fvmEnvironment,
	}
	interpreterEnvironment.Configure(wrappedInterface, codes, cadenceStorage, nil)

	_, err = interpreterEnvironment.ParseAndCheckProgram(
		[]byte(`access(all) fun main() {}`),
		common.ScriptLocation{},
		false,
	)

	interpreterConfig := reflect.ValueOf(interpreterEnvironment).Elem().FieldByName("InterpreterConfig").Interface().(*interpreter.Config)
	checkerConfig := reflect.ValueOf(interpreterEnvironment).Elem().FieldByName("CheckerConfig").Interface().(*sema.Config)

	cadenceRuntime := runtime.NewInterpreterRuntime(runtime.Config{
		AttachmentsEnabled: true,
		Debugger:           debugger,
	})

	fmt.Println("CheckerConfig", checkerConfig)

	standardLibraryValues := stdlib.DefaultScriptStandardLibraryValues(interpreterEnvironment)
	checkerConfig.AccessCheckMode = sema.AccessCheckModeNone

	checker, err := sema.NewChecker(
		nil,
		common.REPLLocation{},
		nil,
		checkerConfig,
	)

	if err != nil {
		return err
	}

	baseActivation := activations.NewActivation(nil, interpreter.BaseActivation)
	for _, value := range standardLibraryValues {
		interpreter.Declare(baseActivation, value)
	}

	/*_, rinter, err := cadenceRuntime.Storage(runtime.Context{
		Interface:   fvmEnvironment,
		Location:    common.ScriptLocation{},
		Environment: interpreterEnvironment,
	})*/

	inter, err := interpreter.NewInterpreter(
		interpreter.ProgramFromChecker(checker),
		checker.Location,
		interpreterConfig,
	)

	r.cadenceRuntime = cadenceRuntime
	r.codesAndPrograms = codes
	r.fvmEnvironment = fvmEnvironment
	r.inter = inter
	r.checker = checker
	r.codes = codesInner
	r.debugger = debugger
	r.interpreterEnvironment = interpreterEnvironment

	return nil
}

func (r *REPL) DebugTransactions(txId flowgo.Identifier) error {

	tx, err := r.storageProvider.GetTransactionById(txId)
	if err != nil {
		r.logger.Err(err).Msgf("cannot get transaction")
		return err
	}
	result, err := r.storageProvider.GetTransactionResult(txId)
	if err != nil {
		r.logger.Err(err).Msgf("cannot get transaction result")
		return err
	}

	err = r.StartAtHeight(result.BlockHeight, tx)
	if err != nil {
		return err
	}

	script := runtime.Script{
		tx.Script,
		tx.Arguments,
	}
	//debug transaction here
	fmt.Println("Debugging transaction", txId)

	r.debugger.RequestPause()
	fmt.Println("Pause requested")

	program, err := r.interpreterEnvironment.ParseAndCheckProgram(script.Source, common.NewTransactionLocation(nil, txId[:]), false)
	fmt.Println(program)
	fmt.Println(err)
	var interactiveDebugger *InteractiveDebugger
	go func() {
		executor := newInterpreterTransactionExecutor(&r.cadenceRuntime, script, runtime.Context{
			Interface:   r.fvmEnvironment,
			Location:    common.NewTransactionLocation(nil, txId[:]),
			Environment: r.interpreterEnvironment,
		})
		err = executor.execute(r.codesAndPrograms)
		if err != nil {
			fmt.Fprintln(r.output, colorizeError(fmt.Sprintf("error: %s", err)))
		}
		fmt.Fprintf(r.output, "Transaction executed\n")

		interactiveDebugger.Continue()
		interactiveDebugger.Exit = true
	}()

	stop := <-r.debugger.Stops()
	fmt.Println("Stopped")

	interactiveDebugger = NewInteractiveDebugger(r.debugger, stop, r.session, r.output, r.codes)
	interactiveDebugger.Run()

	return nil
}

func (r *REPL) onError(err error, location common.Location, codes map[common.Location][]byte) {
	onError := r.OnError
	if onError == nil {
		return
	}
	onError(err, location, codes)
}

func (r *REPL) onExpressionType(expressionType sema.Type) {
	onExpressionType := r.OnExpressionType
	if onExpressionType == nil {
		return
	}
	onExpressionType(expressionType)
}

func (r *REPL) handleCheckerError() error {
	err := r.checker.CheckerError()
	if err == nil {
		return nil
	}

	r.onError(err, r.checker.Location, r.codes)

	return err
}

func isInputComplete(tokens lexer.TokenStream) bool {
	var unmatchedBrackets, unmatchedParens, unmatchedBraces int

	for {

		token := tokens.Next()

		switch token.Type {
		case lexer.TokenBracketOpen:
			unmatchedBrackets++

		case lexer.TokenBracketClose:
			unmatchedBrackets--

		case lexer.TokenParenOpen:
			unmatchedParens++

		case lexer.TokenParenClose:
			unmatchedParens--

		case lexer.TokenBraceOpen:
			unmatchedBraces++

		case lexer.TokenBraceClose:
			unmatchedBraces--
		}

		if token.Is(lexer.TokenEOF) {
			break
		}
	}

	tokens.Revert(0)

	return unmatchedBrackets <= 0 &&
		unmatchedParens <= 0 &&
		unmatchedBraces <= 0
}

var lineSep = []byte{'\n'}

func (r *REPL) Accept(code []byte, eval bool) (inputIsComplete bool, err error) {

	// We need two codes:
	//
	// 1. The code used for parsing and type checking (`code`).
	//
	//    This is only the code that was just entered in the REPL,
	//    as we do not want to re-check and re-run the whole program already previously entered into the REPL –
	//    the checker's and interpreter's state are kept, and they already have the previously entered declarations.
	//
	//    However, just parsing the entered code would result in an AST with wrong position information,
	//    the line number would be always 1. To adjust the line information, we prepend the new code with empty lines.
	//
	// 2. The code used for error pretty printing (`codes`).
	//
	//    We temporarily update the full code of the whole program to include the new code.
	//    This allows the error pretty printer to properly refer to previous code (instead of empty lines),
	//    as well as the new code.
	//    However, if an error occurs, we revert the addition of the new code
	//    and leave the program code as it was before.

	// Append the new code to the existing code (used for error reporting),
	// temporarily, so that errors for the new code can be reported

	currentCode := r.codes[r.checker.Location]

	r.codes[r.checker.Location] = append(currentCode[:], code...)

	defer func() {
		if panicResult := recover(); panicResult != nil {

			var err error

			switch panicResult := panicResult.(type) {
			case goRuntime.Error:
				// don't recover Go or external panics
				panic(panicResult)
			case error:
				err = panicResult
			default:
				err = fmt.Errorf("%s", panicResult)
			}

			r.onError(err, r.checker.Location, r.codes)
		}
	}()

	// If the new code results in a parsing or checking error,
	// reset the code
	defer func() {
		if err != nil {
			r.codes[r.checker.Location] = currentCode
		}
	}()

	// Only parse the new code, and ignore the existing code.
	//
	// Prefix the new code with empty lines,
	// so that the line number is correct in error messages

	lineSepCount := bytes.Count(currentCode, lineSep)

	if lineSepCount > 0 {
		prefixedCode := make([]byte, lineSepCount+len(code))

		for i := 0; i < lineSepCount; i++ {
			prefixedCode[i] = '\n'
		}
		copy(prefixedCode[lineSepCount:], code)

		code = prefixedCode
	}

	tokens, err := lexer.Lex(code, nil)
	defer tokens.Reclaim()
	if err != nil {
		return
	}

	inputIsComplete = isInputComplete(tokens)

	if !inputIsComplete {
		return
	}

	result, errs := parser.ParseStatementsFromTokenStream(nil, tokens, r.parserConfig)
	if len(errs) > 0 {
		err = parser.Error{
			Code:   code,
			Errors: errs,
		}
	}

	if err != nil {
		r.onError(err, r.checker.Location, r.codes)
		return
	}

	r.checker.ResetErrors()

	for _, element := range result {

		switch element := element.(type) {
		case ast.Declaration:
			declaration := element

			program := ast.NewProgram(nil, []ast.Declaration{declaration})

			r.checker.CheckProgram(program)
			err = r.handleCheckerError()
			if err != nil {
				return
			}

			if eval {
				r.inter.VisitProgram(program)
			}

		case ast.Statement:
			statement := element

			r.checker.Program = nil

			var expressionType sema.Type
			expressionStatement, isExpression := statement.(*ast.ExpressionStatement)
			if isExpression {
				expressionType = r.checker.VisitExpression(expressionStatement.Expression, expressionStatement, nil)
				if !eval && expressionType != sema.InvalidType {
					r.onExpressionType(expressionType)
				}
			} else {
				r.checker.CheckStatement(statement)
			}

			err = r.handleCheckerError()
			if err != nil {
				return
			}

			if eval {
				result := ast.AcceptStatement[interpreter.StatementResult](statement, r.inter)

				if result, ok := result.(interpreter.ExpressionResult); ok {
					r.onResult(result)
				}
			}

		default:
			panic(errors.NewUnreachableError())
		}
	}

	return
}

type REPLSuggestion struct {
	Name, Description string
}

func (r *REPL) Suggestions() (result []REPLSuggestion) {
	names := map[string]string{}

	r.checker.Elaboration.ForEachGlobalValue(func(name string, variable *sema.Variable) {
		if names[name] != "" {
			return
		}
		names[name] = variable.Type.String()
	})

	_ = r.checker.Config.BaseValueActivationHandler(nil).ForEach(func(name string, variable *sema.Variable) error {
		if names[name] == "" {
			names[name] = variable.Type.String()
		}
		return nil
	})

	// Iterating over the dictionary of names is safe,
	// as the suggested entries are sorted afterwards

	for name, description := range names { //nolint:maprange
		result = append(result, REPLSuggestion{
			Name:        name,
			Description: description,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		a := result[i]
		b := result[j]
		return a.Name < b.Name
	})

	return
}

func (r *REPL) GetGlobal(name string) interpreter.Value {
	variable := r.inter.Globals.Get(name)
	if variable == nil {
		return nil
	}
	return variable.GetValue(r.inter)
}

func (r *REPL) ExportValue(value interpreter.Value) (cadence.Value, error) {
	return runtime.ExportValue(
		value, r.inter,
		interpreter.LocationRange{
			Location: r.checker.Location,
			// TODO: hasPosition
		},
	)
}

func (r *REPL) onResult(result interpreter.ExpressionResult) {
	onResult := r.OnResult
	if onResult == nil {
		return
	}
	onResult(result)
}
