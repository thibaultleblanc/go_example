package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"
)

func main() {
	scenario := flag.String("scenario", "normal", "Scénario à exécuter: normal, errors, errors-as, cancel, with-cancel, timeout, deadline")
	flag.Parse()

	fmt.Println("=== Concurrent Task Processor ===\n")

	switch *scenario {
	case "normal":
		scenarioNormal()
	case "errors":
		scenarioErrors()
	case "cancel":
		scenarioCancel()
	case "with-cancel":
		scenarioWithCancel()
	case "timeout":
		scenarioTimeout()
	case "deadline":
		scenarioWithDeadline()
	default:
		fmt.Printf("Scénario inconnu: %s\n", *scenario)
		fmt.Println("Scénarios disponibles: normal, errors, errors-as, cancel, with-cancel, timeout, deadline")
	}
}

// scenarioNormal démontre le traitement normal de tâches
func scenarioNormal() {
	fmt.Println("📋 SCÉNARIO 1: Traitement normal")
	fmt.Println("─────────────────────────────────")
	fmt.Println("10 tâches, 3 workers, pas d'erreurs\n")

	processor := NewProcessor(3)
	tasks := []Task{
		NewTask("parse-config", slowWork(80*time.Millisecond)),
		NewTask("fetch-users", slowWork(150*time.Millisecond)),
		NewTask("generate-report", slowWork(200*time.Millisecond)),
		NewTask("send-emails", slowWork(120*time.Millisecond)),
		NewTask("resize-images", slowWork(250*time.Millisecond)),
		NewTask("update-cache", slowWork(90*time.Millisecond)),
		NewTask("export-csv", slowWork(180*time.Millisecond)),
		NewTask("cleanup-temp", slowWork(70*time.Millisecond)),
		NewTask("sync-db", slowWork(160*time.Millisecond)),
		NewTask("notify-webhook", slowWork(110*time.Millisecond)),
	}

	ctx := context.Background()
	start := time.Now()
	results, err := processor.Process(ctx, tasks)
	elapsed := time.Since(start)

	fmt.Printf("\n✅ Traitement terminé en %v\n", elapsed)
	fmt.Printf("   Tâches complétées: %d/%d\n", len(results), len(tasks))
	if err != nil {
		fmt.Printf("   Erreur: %v\n", err)
	}

	// Calculer les statistiques
	var totalDuration time.Duration
	for _, result := range results {
		totalDuration += result.Duration
	}
	fmt.Printf("   Temps total des tâches: %v\n", totalDuration)
	fmt.Printf("   Speedup: %.2fx (temps total / temps réel)\n", float64(totalDuration)/float64(elapsed))
}

// scenarioErrors démontre la gestion des erreurs
func scenarioErrors() {
	fmt.Println("⚠️  SCÉNARIO 2: Gestion des erreurs")
	fmt.Println("────────────────────────────────────")
	fmt.Println("8 tâches, 3 workers, 3 tâches échouent\n")

	processor := NewProcessor(3)
	tasks := []Task{
		NewTask("validate-schema", slowWork(80*time.Millisecond)),
		NewTask("parse-csv-row-42", failingWork(50*time.Millisecond, "malformed CSV: missing field 'email'")),
		NewTask("fetch-user-data", slowWork(120*time.Millisecond)),
		NewTask("call-payment-api", failingWork(200*time.Millisecond, "HTTP 503: payment gateway unavailable")),
		NewTask("update-inventory", slowWork(90*time.Millisecond)),
		NewTask("send-notification", failingWork(80*time.Millisecond, "SMTP: connection refused")),
		NewTask("write-audit-log", slowWork(60*time.Millisecond)),
		NewTask("flush-cache", slowWork(100*time.Millisecond)),
	}

	ctx := context.Background()
	results, err := processor.Process(ctx, tasks)

	fmt.Printf("\n✅ Traitement terminé\n")
	fmt.Printf("   Tâches: %d/%d\n", len(results), len(tasks))

	// Afficher le résumé des erreurs
	if err != nil {
		fmt.Printf("   ❌ Erreur globale: %v\n\n", err)
	}

	// Détails des erreurs
	fmt.Println("   📊 Résultats individuels:")
	successCount := 0
	failCount := 0
	for _, result := range results {
		if result.Err != nil {
			failCount++
			// errors.As extrait l'erreur wrappée dans son type concret
			var taskErr *ErrTaskFailed
			if errors.As(result.Err, &taskErr) {
				fmt.Printf("     ✗ %s: erreur métier - %v\n", result.TaskID, result.Err)
			}
		} else {
			successCount++
		}
	}
	fmt.Printf("\n   Résumé: %d succès, %d échecs\n", successCount, failCount)

	// errors.Join retourne une erreur qui agrège toutes les sous-erreurs.
	// errors.As parcourt l'arbre d'erreurs et retourne la première correspondance.
	if err != nil {
		var taskErr *ErrTaskFailed
		if errors.As(err, &taskErr) {
			fmt.Printf("\n   → errors.As sur l'erreur joinée: première ErrTaskFailed trouvée: %q (raison: %v)\n", taskErr.TaskID, taskErr.Err)
		}
	}
}

// scenarioCancel démontre la cancellation et l'arrêt gracieux
func scenarioCancel() {
	fmt.Println("⏸️  SCÉNARIO 3: Cancellation et arrêt gracieux")
	fmt.Println("──────────────────────────────────────────────")
	fmt.Println("15 tâches longues, 3 workers, cancellation après 1 seconde\n")

	processor := NewProcessor(3)
	tasks := make([]Task, 15)
	for i := 0; i < 15; i++ {
		tasks[i] = NewTask(
			fmt.Sprintf("export-batch-%d", i+1),
			slowWork(500*time.Millisecond),
		)
	}

	// Créer un contexte qui s'annule après 1 seconde
	// context.WithTimeout = sucre syntaxique autour de WithDeadline
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	fmt.Println("▶️  Lancement du traitement...")
	fmt.Println("   (Les workers vont s'arrêter proprement après ~1 seconde)\n")

	start := time.Now()
	results, err := processor.Process(ctx, tasks)
	elapsed := time.Since(start)

	fmt.Printf("\n⏹️  Traitement interrompu après %v\n", elapsed)
	fmt.Printf("   Tâches complétées: %d/%d\n", len(results), len(tasks))
	if err != nil {
		fmt.Printf("   Raison: %v\n", err)
	}

	// Compter les tâches annulées
	canceledCount := 0
	successCount := 0
	for _, result := range results {
		if errors.Is(result.Err, ErrCanceled) {
			canceledCount++
		} else if result.Err == nil {
			successCount++
		}
	}
	fmt.Printf("\n   Tâches réussies: %d\n", successCount)
	fmt.Printf("   Tâches annulées: %d\n", canceledCount)
	fmt.Printf("\n   ✓ Les workers se sont arrêtés proprement (pas de goroutines orphelines)\n")
}

// scenarioWithCancel illustre context.WithCancel : annulation manuelle et explicite
func scenarioWithCancel() {
	fmt.Println("🛑 SCÉNARIO 3b: context.WithCancel — annulation manuelle")
	fmt.Println("─────────────────────────────────────────────────────")
	fmt.Println("Contrairement à WithTimeout (durée fixe), WithCancel donne")
	fmt.Println("un contrôle total : on appelle cancel() quand ON le décide.")
	fmt.Println("Usage typique : signal SIGTERM, première erreur critique, logique métier.\n")

	processor := NewProcessor(3)
	tasks := make([]Task, 15)
	for i := range tasks {
		tasks[i] = NewTask(
			fmt.Sprintf("batch-job-%d", i+1),
			slowWork(500*time.Millisecond),
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Goroutine qui simule un signal externe d'arrêt
	go func() {
		time.Sleep(1200 * time.Millisecond)
		fmt.Println("   [Signal externe] Arrêt demandé → cancel()")
		cancel() // on décide explicitement d'arrêter
	}()

	fmt.Println("▶️  Lancement du traitement...")
	fmt.Println("   (annulation manuelle dans ~1.2s)\n")

	start := time.Now()
	results, err := processor.Process(ctx, tasks)
	elapsed := time.Since(start)

	fmt.Printf("\n🛑 Traitement interrompu après %v\n", elapsed)
	fmt.Printf("   Tâches complétées: %d/%d\n", len(results), len(tasks))
	if err != nil {
		fmt.Printf("   Raison: %v\n", err)
	}
	// ctx.Err() retourne la cause : context.Canceled ou context.DeadlineExceeded
	if ctx.Err() == context.Canceled {
		fmt.Println("   → ctx.Err() == context.Canceled (annulation manuelle confirmée)")
	}

	canceledCount := 0
	successCount := 0
	for _, result := range results {
		if errors.Is(result.Err, ErrCanceled) {
			canceledCount++
		} else if result.Err == nil {
			successCount++
		}
	}
	fmt.Printf("\n   Tâches réussies: %d\n", successCount)
	fmt.Printf("   Tâches annulées: %d\n", canceledCount)
}

// scenarioTimeout démontre les timeouts
func scenarioTimeout() {
	fmt.Println("⏱️  SCÉNARIO 4: Timeout sur deadline")
	fmt.Println("─────────────────────────────────────")
	fmt.Println("5 tâches longues, 2 workers, deadline de 800ms (tâches de 500ms chacune)\n")

	processor := NewProcessor(2)
	tasks := []Task{
		NewTask("index-documents-1", slowWork(500*time.Millisecond)),
		NewTask("index-documents-2", slowWork(500*time.Millisecond)),
		NewTask("index-documents-3", slowWork(500*time.Millisecond)),
		NewTask("index-documents-4", slowWork(500*time.Millisecond)),
		NewTask("index-documents-5", slowWork(500*time.Millisecond)),
	}

	// Créer un contexte avec deadline
	// context.WithTimeout = sucre syntaxique : WithTimeout(ctx, d) == WithDeadline(ctx, time.Now().Add(d))
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	fmt.Println("▶️  Lancement du traitement...")
	fmt.Println("   (Le deadline s'approche progressivement)\n")

	start := time.Now()
	results, err := processor.Process(ctx, tasks)
	elapsed := time.Since(start)

	fmt.Printf("\n⏱️  Traitement arrêté après %v (deadline: 800ms)\n", elapsed)
	fmt.Printf("   Tâches complétées: %d/%d\n", len(results), len(tasks))
	if err != nil {
		fmt.Printf("   Erreur: %v\n", err)
	}

	// Analyser les résultats
	completedCount := 0
	timeoutCount := 0
	for _, result := range results {
		if errors.Is(result.Err, ErrTimeout) {
			timeoutCount++
		} else if result.Err == nil {
			completedCount++
		}
	}

	fmt.Printf("\n   📊 Résultats:\n")
	fmt.Printf("      - Complétées: %d tâches\n", completedCount)
	fmt.Printf("      - Timeout: %d tâches\n", timeoutCount)
	fmt.Printf("\n   ℹ️  Les tâches déjà lancées se sont arrêtées dès qu'elles ont détecté le timeout\n")
}

// scenarioWithDeadline illustre context.WithDeadline : deadline absolue (heure fixe)
func scenarioWithDeadline() {
	fmt.Println("⏰ SCÉNARIO 4b: context.WithDeadline — deadline absolue")
	fmt.Println("─────────────────────────────────────────────────────")
	fmt.Println("Contrairement à WithTimeout (durée relative), WithDeadline")
	fmt.Println("prend une heure absolue. Utile pour partager une deadline entre")
	fmt.Println("plusieurs services ou respecter un SLA fixé à l'avance.\n")

	processor := NewProcessor(2)
	tasks := []Task{
		NewTask("index-documents-1", slowWork(500*time.Millisecond)),
		NewTask("index-documents-2", slowWork(500*time.Millisecond)),
		NewTask("index-documents-3", slowWork(500*time.Millisecond)),
		NewTask("index-documents-4", slowWork(500*time.Millisecond)),
		NewTask("index-documents-5", slowWork(500*time.Millisecond)),
	}

	// WithDeadline prend une heure absolue — pas une durée
	deadline := time.Now().Add(800 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	fmt.Printf("▶️  Lancement (deadline absolue: %s)\n\n", deadline.Format("15:04:05.000"))

	results, err := processor.Process(ctx, tasks)

	fmt.Printf("\n⏰ Traitement arrêté\n")
	fmt.Printf("   Tâches complétées: %d/%d\n", len(results), len(tasks))
	if err != nil {
		fmt.Printf("   Erreur: %v\n", err)
	}
	// ctx.Err() est toujours context.DeadlineExceeded pour WithDeadline ET WithTimeout
	if ctx.Err() == context.DeadlineExceeded {
		fmt.Println("   → ctx.Err() == context.DeadlineExceeded")
	}
	// ctx.Deadline() retourne la deadline et si elle était définie
	if dl, ok := ctx.Deadline(); ok {
		fmt.Printf("   → ctx.Deadline() = %s (expirée il y a %v)\n",
			dl.Format("15:04:05.000"), time.Since(dl).Round(time.Millisecond))
	}
}

// Concepts clés à comprendre
/*
═══════════════════════════════════════════════════════════════════════

1. CONTEXT
──────────
Context est un objet qui porte:
- Une deadline (WithDeadline)
- Un timeout (WithTimeout = deadline automatique)
- Une cancellation (WithCancel)
- Des valeurs métier (WithValue)

À chaque point du code, vérifier: "Le contexte est-il terminé ?"
→ select { case <-ctx.Done(): ... }

2. CONCURRENCE
──────────────
Goroutines = threads légers (milions possibles)
Canaux = communication synchrone entre goroutines
WaitGroup = attendre que des goroutines se terminent

Pattern Worker Pool:
  - M tasks → taskChan → N workers → resultChan → agrégation

3. GESTION D'ERREURS
────────────────────
errors.Is()   : vérifier si une erreur (ou une wrapée) correspond à une valeur
errors.As()   : extraire l'erreur wrappée dans son type concret
errors.Join() : combiner plusieurs erreurs en une (Go 1.20+) — préserve Is/As
fmt.Errorf("%w", err) : wrapper avec contexte
Erreurs métier ≠ erreurs système

═══════════════════════════════════════════════════════════════════════
*/
