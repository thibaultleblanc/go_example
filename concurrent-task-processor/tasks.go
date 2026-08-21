package main

import (
	"context"
	"fmt"
	"time"
)

// simpleTask est l'implémentation concrète de Task basée sur un callback.
// Elle est non-exportée : utiliser NewTask pour créer une Task.
type simpleTask struct {
	id  string
	run func(ctx context.Context) error
}

func (t simpleTask) ID() string                    { return t.id }
func (t simpleTask) Run(ctx context.Context) error { return t.run(ctx) }

// NewTask crée une Task à partir d'un identifiant et d'un callback.
// C'est le même pattern que http.HandlerFunc : un adaptateur entre une
// fonction ordinaire et l'interface Task.
func NewTask(id string, run func(ctx context.Context) error) Task {
	return simpleTask{id: id, run: run}
}

// slowWork retourne un callback simulant un travail de durée fixe.
// Il s'arrête immédiatement si le contexte est annulé (Canceled ou DeadlineExceeded).
func slowWork(d time.Duration) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
			return nil
		}
	}
}

// failingWork retourne un callback qui échoue après une durée donnée.
// Utile pour simuler des erreurs métier réalistes (API indisponible, donnée invalide…)
func failingWork(d time.Duration, reason string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
			return fmt.Errorf("%s", reason)
		}
	}
}
