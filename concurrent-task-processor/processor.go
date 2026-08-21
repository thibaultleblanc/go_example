package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Task est l'interface que doit respecter toute unité de travail soumise au Processor.
// Ce pattern est identique à http.Handler dans la stdlib : le consommateur définit
// l'interface, et n'importe quelle valeur qui l'implémente peut être traitée.
type Task interface {
	ID() string
	Run(ctx context.Context) error
}

// Result représente le résultat du traitement d'une tâche
type Result struct {
	TaskID    string
	Err       error
	Duration  time.Duration
	Timestamp time.Time
}

// Processor traite les tâches de manière concurrente
type Processor struct {
	workerCount int
	taskChan    chan Task
	resultChan  chan Result
}

// NewProcessor crée un nouveau processeur avec N workers
func NewProcessor(workerCount int) *Processor {
	return &Processor{
		workerCount: workerCount,
		taskChan:    make(chan Task, workerCount*2),
		resultChan:  make(chan Result, workerCount*2),
	}
}

// Process traite une liste de tâches de manière concurrente
//
// Le pattern utilisé ici est le "worker pool" :
// - Main goroutine envoie les tâches dans taskChan
// - N workers lisent du taskChan et traitent les tâches
// - Chaque worker envoie le résultat dans resultChan
// - Main goroutine collecte les résultats
//
// Propagation du contexte :
// - À chaque étape, on vérifie ctx.Done()
// - Si le contexte est annulé, les workers s'arrêtent proprement
// - Pas de goroutines "orphelines"
func (p *Processor) Process(ctx context.Context, tasks []Task) ([]Result, error) {
	// Vérifier que le contexte n'est pas déjà annulé
	select {
	case <-ctx.Done():
		return nil, ErrCanceled
	default:
	}

	var wg sync.WaitGroup
	results := make([]Result, 0, len(tasks))
	resultsMutex := sync.Mutex{}

	// Goroutine qui collecte les résultats (dans une goroutine séparée)
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for result := range p.resultChan {
			resultsMutex.Lock()
			results = append(results, result)
			resultsMutex.Unlock()
		}
	}()

	// Lancer N workers
	// Chaque worker s'exécute dans sa propre goroutine
	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			p.worker(ctx, workerID)
		}(i)
	}

	// Goroutine qui envoie les tâches
	// Elle peut être interrompue par la cancellation du contexte
	go func() {
		for _, task := range tasks {
			select {
			case <-ctx.Done():
				// Contexte annulé : arrêter l'envoi des tâches
				close(p.taskChan)
				return
			case p.taskChan <- task:
				// Tâche envoyée
			}
		}
		// Toutes les tâches ont été envoyées
		close(p.taskChan)
	}()

	// Attendre que tous les workers aient terminé
	wg.Wait()

	// Fermer le canal des résultats
	// Cela signale au collecteur que tous les résultats ont été envoyés
	close(p.resultChan)

	// Attendre que le collecteur ait fini de lire tous les résultats
	<-collectorDone

	// Vérifier s'il y a eu des erreurs
	var errorCollector ErrorCollector
	for _, result := range results {
		if result.Err != nil {
			errorCollector.Add(result.Err)
		}
	}

	if errorCollector.HasErrors() {
		// errors.Join (Go 1.20+) combine plusieurs erreurs en une seule.
		// Contrairement à fmt.Errorf, l'erreur jointe supporte errors.Is et errors.As
		// sur chacune des sous-erreurs : on ne perd aucune information.
		return results, errors.Join(errorCollector.All()...)
	}

	return results, nil
}

// worker traite les tâches du canal taskChan
// Pattern clé : boucle infinie avec select sur le contexte
func (p *Processor) worker(ctx context.Context, workerID int) {
	for {
		select {
		case <-ctx.Done():
			// Le contexte a été annulé ou est arrivé à expiration
			// Le worker s'arrête proprement sans laisser de goroutine
			fmt.Printf("[Worker %d] ⚠️ Stopped (context canceled)\n", workerID)
			return

		// ⚠️ CRUCIAL: toujours récupérer le 2e paramètre (ok) en lisant d'un canal
		// Sans ok, on ne distingue pas une tâche vide (Task{}) du signal de fermeture
		// Risque: boucle infinie ou traitement de tâches fantômes après fermeture
		case task, ok := <-p.taskChan:
			// ok=false signifie que le canal a été fermé
			if !ok {
				fmt.Printf("[Worker %d] ✓ No more tasks, exiting\n", workerID)
				return
			}

			// Traiter la tâche
			result := p.processTask(ctx, workerID, task)
			if result.Err == nil {
				fmt.Printf("[Worker %d] ✓ Task %s completed in %v\n", workerID, task.ID(), result.Duration)
			} else {
				fmt.Printf("[Worker %d] ✗ Task %s failed: %v\n", workerID, task.ID(), result.Err)
			}

			// Envoyer le résultat
			// Note: le contexte peut être annulé à ce moment-là aussi
			select {
			case <-ctx.Done():
				// Contexte annulé, sortir sans envoyer le résultat
				return
			case p.resultChan <- result:
				// Résultat envoyé
			}
		}
	}
}

// processTask exécute le callback de la tâche et mappe les erreurs de contexte
// vers nos types d'erreurs métier.
//
// La responsabilité de respecter le contexte (ctx.Done, time.After) est
// déléguée au callback lui-même — c'est le pattern idiomatique en Go.
func (p *Processor) processTask(ctx context.Context, workerID int, task Task) Result {
	start := time.Now()
	result := Result{
		TaskID:    task.ID(),
		Timestamp: start,
	}

	err := task.Run(ctx)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			result.Err = ErrTimeout
		case errors.Is(err, context.Canceled):
			result.Err = ErrCanceled
		default:
			// Erreur métier : on wrappe avec l'identifiant de la tâche
			result.Err = &ErrTaskFailed{TaskID: task.ID(), Err: err}
		}
	}

	result.Duration = time.Since(start)
	return result
}
