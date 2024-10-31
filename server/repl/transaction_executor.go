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
	"github.com/onflow/cadence/runtime"
	"sync"

	"github.com/onflow/cadence"
	"github.com/onflow/cadence/errors"
	"github.com/onflow/cadence/interpreter"
	"github.com/onflow/cadence/sema"
)

type interpreterTransactionExecutorPreparation struct {
	codesAndPrograms runtime.CodesAndPrograms
	environment      runtime.Environment
	preprocessErr    error
	transactionType  *sema.TransactionType
	storage          *runtime.Storage
	program          *interpreter.Program
	preprocessOnce   sync.Once
}

type interpreterTransactionExecutorExecution struct {
	executeErr  error
	interpret   runtime.InterpretFunc
	executeOnce sync.Once
}

type interpreterTransactionExecutor struct {
	context runtime.Context
	interpreterTransactionExecutorExecution
	runtime *runtime.Runtime
	script  runtime.Script
	interpreterTransactionExecutorPreparation
}

func newInterpreterTransactionExecutor(
	runtime *runtime.Runtime,
	script runtime.Script,
	context runtime.Context,
) *interpreterTransactionExecutor {

	return &interpreterTransactionExecutor{
		runtime: runtime,
		script:  script,
		context: context,
	}
}

func (executor *interpreterTransactionExecutor) preprocess(codesAndPrograms runtime.CodesAndPrograms) (err error) {
	context := executor.context
	location := context.Location
	script := executor.script

	executor.codesAndPrograms = codesAndPrograms

	runtimeInterface := context.Interface

	storage := runtime.NewStorage(runtimeInterface, runtimeInterface)
	executor.storage = storage

	environment := context.Environment

	environment.Configure(
		runtimeInterface,
		codesAndPrograms,
		storage,
		context.CoverageReport,
	)
	executor.environment = environment

	program, err := environment.ParseAndCheckProgram(
		script.Source,
		location,
		true,
	)
	if err != nil {
		return fmt.Errorf("preprocessing error: %w", err)
	}
	executor.program = program

	transactions := program.Elaboration.TransactionTypes
	transactionCount := len(transactions)
	if transactionCount != 1 {
		err = runtime.InvalidTransactionCountError{
			Count: transactionCount,
		}
		return fmt.Errorf("preprocessing error: %w", err)
	}

	transactionType := transactions[0]
	executor.transactionType = transactionType

	var authorizerAddresses []runtime.Address
	errors.WrapPanic(func() {
		authorizerAddresses, err = runtimeInterface.GetSigningAccounts()
	})
	if err != nil {
		return fmt.Errorf("preprocessing error: %w", err)
	}

	// check parameter count

	argumentCount := len(script.Arguments)
	authorizerCount := len(authorizerAddresses)

	transactionParameterCount := len(transactionType.Parameters)
	if argumentCount != transactionParameterCount {
		err = runtime.InvalidEntryPointParameterCountError{
			Expected: transactionParameterCount,
			Actual:   argumentCount,
		}
		return fmt.Errorf("preprocessing error: %w", err)
	}

	prepareParameters := transactionType.PrepareParameters

	transactionAuthorizerCount := len(prepareParameters)
	if authorizerCount != transactionAuthorizerCount {
		err = runtime.InvalidTransactionAuthorizerCountError{
			Expected: transactionAuthorizerCount,
			Actual:   authorizerCount,
		}
		return fmt.Errorf("preprocessing error: %w", err)
	}

	// gather authorizers

	executor.interpret = executor.transactionExecutionFunction(
		func(inter *interpreter.Interpreter) []interpreter.Value {
			return executor.authorizerValues(
				inter,
				authorizerAddresses,
				prepareParameters,
			)
		},
	)

	return nil
}

func (executor *interpreterTransactionExecutor) authorizerValues(
	inter *interpreter.Interpreter,
	addresses []runtime.Address,
	parameters []sema.Parameter,
) []interpreter.Value {

	// gather authorizers

	authorizerValues := make([]interpreter.Value, 0, len(addresses))

	for i, address := range addresses {
		parameter := parameters[i]

		addressValue := interpreter.NewAddressValue(inter, address)

		accountValue := executor.environment.NewAccountValue(inter, addressValue)

		referenceType, ok := parameter.TypeAnnotation.Type.(*sema.ReferenceType)
		if !ok || referenceType.Type != sema.AccountType {
			panic(errors.NewUnreachableError())
		}

		authorization := interpreter.ConvertSemaAccessToStaticAuthorization(
			inter,
			referenceType.Authorization,
		)

		accountReferenceValue := interpreter.NewEphemeralReferenceValue(
			inter,
			authorization,
			accountValue,
			sema.AccountType,
			// okay to pass an empty range here because the account value is never a reference, so this can't fail
			interpreter.EmptyLocationRange,
		)

		authorizerValues = append(authorizerValues, accountReferenceValue)
	}

	return authorizerValues
}

func (executor *interpreterTransactionExecutor) execute(codes runtime.CodesAndPrograms) (err error) {
	err = executor.preprocess(codes)
	if err != nil {
		return err
	}

	environment := executor.environment
	context := executor.context
	location := context.Location

	_, inter, err := environment.Interpret(
		location,
		executor.program,
		executor.interpret,
	)
	if err != nil {
		return fmt.Errorf("execution error: %w", err)
	}

	// Write back all stored values, which were actually just cached, back into storage
	err = environment.CommitStorage(inter)
	if err != nil {
		return fmt.Errorf("execution error: %w", err)
	}

	return nil
}

func hasValidStaticType(inter *interpreter.Interpreter, value interpreter.Value) bool {
	switch value := value.(type) {
	case *interpreter.ArrayValue:
		return value.Type != nil
	case *interpreter.DictionaryValue:
		return value.Type.KeyType != nil &&
			value.Type.ValueType != nil
	default:
		// For other values, static type is NOT inferred.
		// Hence no need to validate it here.
		return value.StaticType(inter) != nil
	}
}

func validateArgumentParams(
	inter *interpreter.Interpreter,
	decoder runtime.ArgumentDecoder,
	locationRange interpreter.LocationRange,
	arguments [][]byte,
	parameters []sema.Parameter,
) (
	[]interpreter.Value,
	error,
) {
	argumentCount := len(arguments)
	parameterCount := len(parameters)

	if argumentCount != parameterCount {
		return nil, runtime.InvalidEntryPointParameterCountError{
			Expected: parameterCount,
			Actual:   argumentCount,
		}
	}

	argumentValues := make([]interpreter.Value, len(arguments))

	// Decode arguments against parameter types
	for parameterIndex, parameter := range parameters {
		parameterType := parameter.TypeAnnotation.Type
		argument := arguments[parameterIndex]

		exportedParameterType := runtime.ExportMeteredType(inter, parameterType, map[sema.TypeID]cadence.Type{})
		var value cadence.Value
		var err error

		errors.WrapPanic(func() {
			value, err = decoder.DecodeArgument(
				argument,
				exportedParameterType,
			)
		})

		if err != nil {
			return nil, &runtime.InvalidEntryPointArgumentError{
				Index: parameterIndex,
				Err:   err,
			}
		}

		var arg interpreter.Value
		panicError := runtime.UserPanicToError(func() {
			// if importing an invalid public key, this call panics
			arg, err = runtime.ImportValue(
				inter,
				locationRange,
				decoder,
				decoder.ResolveLocation,
				value,
				parameterType,
			)
		})

		if panicError != nil {
			return nil, &runtime.InvalidEntryPointArgumentError{
				Index: parameterIndex,
				Err:   panicError,
			}
		}

		if err != nil {
			return nil, &runtime.InvalidEntryPointArgumentError{
				Index: parameterIndex,
				Err:   err,
			}
		}

		// Ensure the argument is of an importable type
		argType := arg.StaticType(inter)

		if !arg.IsImportable(inter, locationRange) {
			return nil, &runtime.ArgumentNotImportableError{
				Type: argType,
			}
		}

		// Check that decoded value is a subtype of static parameter type
		if !inter.IsSubTypeOfSemaType(argType, parameterType) {
			return nil, &runtime.InvalidEntryPointArgumentError{
				Index: parameterIndex,
				Err: &runtime.InvalidValueTypeError{
					ExpectedType: parameterType,
				},
			}
		}

		// Check whether the decoded value conforms to the type associated with the value
		if !arg.ConformsToStaticType(
			inter,
			interpreter.EmptyLocationRange,
			interpreter.TypeConformanceResults{},
		) {
			return nil, &runtime.InvalidEntryPointArgumentError{
				Index: parameterIndex,
				Err: &runtime.MalformedValueError{
					ExpectedType: parameterType,
				},
			}
		}

		// Ensure static type info is available for all values
		interpreter.InspectValue(
			inter,
			arg,
			func(value interpreter.Value) bool {
				if value == nil {
					return true
				}

				if !hasValidStaticType(inter, value) {
					panic(errors.NewUnexpectedError("invalid static type for argument: %d", parameterIndex))
				}

				return true
			},
			locationRange,
		)

		argumentValues[parameterIndex] = arg
	}

	return argumentValues, nil
}

func (executor *interpreterTransactionExecutor) transactionExecutionFunction(
	authorizerValues func(*interpreter.Interpreter) []interpreter.Value,
) runtime.InterpretFunc {
	return func(inter *interpreter.Interpreter) (value interpreter.Value, err error) {

		// Recover internal panics and return them as an error.
		// For example, the argument validation might attempt to
		// load contract code for non-existing types

		defer inter.RecoverErrors(func(internalErr error) {
			err = internalErr
		})

		values, err := validateArgumentParams(
			inter,
			executor.environment,
			interpreter.EmptyLocationRange,
			executor.script.Arguments,
			executor.transactionType.Parameters,
		)
		if err != nil {
			return nil, err
		}

		values = append(values, authorizerValues(inter)...)
		err = inter.InvokeTransaction(0, values...)
		return nil, err
	}
}
