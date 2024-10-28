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
	"github.com/onflow/cadence"
	"github.com/onflow/cadence/ast"
	"github.com/onflow/cadence/common"
	"github.com/onflow/cadence/errors"
	"github.com/onflow/cadence/interpreter"
	"github.com/onflow/cadence/parser"
	"github.com/onflow/cadence/parser/lexer"
	"github.com/onflow/cadence/runtime"
	"github.com/onflow/cadence/sema"
	"reflect"
	goRuntime "runtime"
	"sort"
	"strings"
)

type REPL struct {
	inter            *interpreter.Interpreter
	checker          *sema.Checker
	environment      runtime.Environment
	debugger         *interpreter.Debugger
	OnError          func(err error, location runtime.Location, codes map[runtime.Location][]byte)
	OnExpressionType func(sema.Type)
	OnResult         func(interpreter.Value)
	codes            map[runtime.Location][]byte
	parserConfig     parser.Config
}

func Interpret(inter *interpreter.Interpreter) (interpreter.Value, error) {
	return inter.Invoke("main")
}

func NewREPL(runtimeInterface runtime.Interface) (*REPL, error) {

	// Prepare checkers
	codesAndPrograms := runtime.NewCodesAndPrograms()
	debugger := interpreter.NewDebugger()

	config := runtime.Config{
		AttachmentsEnabled: true,
		Debugger:           debugger,
	}

	storage := runtime.NewStorage(runtimeInterface, runtimeInterface)
	environment := runtime.NewScriptInterpreterEnvironment(config)
	environment.Configure(
		runtimeInterface,
		codesAndPrograms,
		storage,
		nil,
	)

	rv := reflect.ValueOf(environment)
	rv = rv.Elem()                       // deref *rpc.Client
	rv = rv.FieldByName("CheckerConfig") // get "codec" field from rpc.Client
	checkerConfig := rv.Interface().(*sema.Config)
	checkerConfig.AccessCheckMode = sema.AccessCheckModeNone

	checker, err := sema.NewChecker(
		nil,
		common.ScriptLocation{},
		nil,
		checkerConfig,
	)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	afterCh := make(chan struct{})

	var debuggerInterpreter *interpreter.Interpreter
	go func() {
		fmt.Println("waiting debugger")
		stop := debugger.Pause()
		fmt.Println("debugger stopped")

		debuggerInterpreter = stop.Interpreter
		debugger.Continue()

		afterCh <- struct{}{}
		fmt.Println("interpreter ready")
	}()

	program, err := environment.ParseAndCheckProgram(
		[]byte(`access(all) fun main() {getAccount(0x7e60df042a9c0868).balance;}`),
		common.ScriptLocation{},
		false,
	)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	fmt.Println("executing empty code")
	//execute empty code
	go environment.Interpret(checker.Location, program, Interpret)
	fmt.Println("executed empty code")

	for {
		select {

		case <-afterCh:

			fmt.Println("got interpreter")

			inter, _ := interpreter.NewInterpreterWithSharedState(
				interpreter.ProgramFromChecker(checker),
				checker.Location,
				debuggerInterpreter.SharedState,
			)
			inter.SharedState.Config.Storage = storage
			return &REPL{
				inter:        inter,
				environment:  environment,
				debugger:     debugger,
				checker:      checker,
				codes:        map[runtime.Location][]byte{},
				parserConfig: parser.Config{},
			}, nil
		}
	}

}

func (r *REPL) onError(err error, location common.Location, codes map[runtime.Location][]byte) {
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

func (r *REPL) Suggestions(word string) (result []REPLSuggestion) {
	names := map[string]string{}

	if strings.Contains(word, ".") {
		words := strings.Split(word, ".")

		code := []byte(strings.Join(words[:len(words)-1], "."))
		fmt.Println("code", string(code))
		tokens, err := lexer.Lex(code, nil)
		defer tokens.Reclaim()
		if err != nil {
			fmt.Println("lexer error", err)
			return
		}

		inputIsComplete := isInputComplete(tokens)

		if !inputIsComplete {
			fmt.Println("input not complete")
			return
		}

		parsed, errs := parser.ParseStatementsFromTokenStream(nil, tokens, r.parserConfig)
		if len(errs) > 0 {
			fmt.Println("parse error", errs)
			return
		}

		r.checker.ResetErrors()

		for _, element := range parsed {

			switch element := element.(type) {
			case ast.Declaration:
				fmt.Println("declaration")
				return

			case ast.Statement:
				fmt.Println("statement")
				statement := element

				r.checker.Program = nil

				var expressionType sema.Type
				expressionStatement, isExpression := statement.(*ast.ExpressionStatement)
				if !isExpression {
					fmt.Println("not expression")
					return
				}
				expressionType = r.checker.VisitExpression(expressionStatement.Expression, expressionStatement, nil)

				memberResolver := expressionType.GetMembers()
				for name, member := range memberResolver {
					fmt.Println("name", name)
					m := member.Resolve(nil, name, ast.Range{}, func(err error) {
						fmt.Println(err)
					})
					fmt.Println("after resolve")
					fmt.Println(name, m.TypeAnnotation.String())
					fmt.Println("after docstring")
					names[name] = m.DocString
				}

			default:
				panic(errors.NewUnreachableError())
			}
		}

	} else {
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
	}
	fmt.Println("word", word)

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
