package main

import (
	"errors"
	"fmt"
)

// ErrTaskFailed représente une erreur lors du traitement d'une tâche
type ErrTaskFailed struct {
	TaskID string
	Err    error
}

func (e *ErrTaskFailed) Error() string {
	return fmt.Sprintf("task %s failed: %v", e.TaskID, e.Err)
}

// Is permet de vérifier le type d'erreur avec errors.Is()
func (e *ErrTaskFailed) Is(target error) bool {
	_, ok := target.(*ErrTaskFailed)
	return ok
}

// Unwrap retourne l'erreur sous-jacente
func (e *ErrTaskFailed) Unwrap() error {
	return e.Err
}

// ErrTimeout indique que le contexte a dépassé son deadline
var ErrTimeout = errors.New("context deadline exceeded")

// ErrCanceled indique que le contexte a été annulé
var ErrCanceled = errors.New("processing canceled by context")

// ErrorCollector agrège plusieurs erreurs
type ErrorCollector struct {
	errors []error
}

func (ec *ErrorCollector) Add(err error) {
	if err != nil {
		ec.errors = append(ec.errors, err)
	}
}

func (ec *ErrorCollector) HasErrors() bool {
	return len(ec.errors) > 0
}

func (ec *ErrorCollector) Error() string {
	if len(ec.errors) == 0 {
		return ""
	}
	if len(ec.errors) == 1 {
		return ec.errors[0].Error()
	}
	return fmt.Sprintf("%d errors occurred:\n", len(ec.errors))
}

func (ec *ErrorCollector) All() []error {
	return ec.errors
}

// CollectErrors combine plusieurs erreurs
func CollectErrors(errs ...error) error {
	var collector ErrorCollector
	for _, err := range errs {
		collector.Add(err)
	}
	if collector.HasErrors() {
		return &collector
	}
	return nil
}
