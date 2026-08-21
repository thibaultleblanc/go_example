# Concurrent Task Processor

Un service de traitement de tâches concurrentes qui démontre les trois piliers fondamentaux en Go :

1. **Concurrence** : goroutines, coordination, worker pools
2. **Context** : propagation de la cancellation, timeouts, valeurs
3. **Gestion des erreurs** : errors.Is, errors.As, error wrapping, propagation

## Concepts couverts

### 1. Context et propagation

- ✅ `context.Background()` et `context.TODO()`
- ✅ `context.WithCancel()` - cancellation gracieuse
- ✅ `context.WithTimeout()` - timeouts automatiques
- ✅ `context.WithDeadline()` - deadline absolue
- ✅ Propagation à travers les goroutines
- ✅ `ctx.Done()` - canal de cancellation

### 2. Concurrence et patterns

- ✅ **Worker Pool** : N workers traitent M tâches
- ✅ **Fan-out / Fan-in** : distribuer et agréger
- ✅ **Coordination** : `sync.WaitGroup`, canaux
- ✅ **Race conditions** : design thread-safe
- ✅ **Graceful shutdown** : arrêt propre des goroutines

### 3. Gestion des erreurs

- ✅ Custom error types
- ✅ `errors.Is()` et `errors.As()`
- ✅ Error wrapping avec `fmt.Errorf("%w")`
- ✅ Erreurs concurrentes et agrégation
- ✅ Distinction erreur métier / erreur système

## Architecture

### Modèle de programmation : l'interface `Task`

Le cœur du design suit le pattern idiomatique Go : l'interface est définie par le **consommateur** (le `Processor`), pas le producteur :

```go
// Interface définie par le Processor
type Task interface {
    ID() string                    // identifiant unique
    Run(ctx context.Context) error // callback qui respecte le contexte
}
```

Bénéfices :

- **Extensibilité** : n'importe quelle struct peut implémenter `Task`
- **Testabilité** : facile de créer des mocks
- **Idiomatique** : même pattern que `http.Handler`, `io.Reader`, etc.

### Implémentation concrète : `simpleTask`

Dans `tasks.go`, on fournit une implémentation simple via un constructeur :

```go
type simpleTask struct {
    id  string
    run func(ctx context.Context) error
}

// Constructeur = adaptateur (comme http.HandlerFunc)
func NewTask(id string, run func(ctx context.Context) error) Task {
    return simpleTask{id: id, run: run}
}
```

### `Processor` - le cœur du système

```go
type Processor struct {
    workerCount int
    taskChan    chan Task      // canal de tâches (fan-out)
    resultChan  chan Result    // canal de résultats (fan-in)
}

func (p *Processor) Process(ctx context.Context, tasks []Task) ([]Result, error)
```

**Flux** :

1. Main goroutine crée N workers
2. Workers lisent du `taskChan` dans une boucle
3. Chaque worker appelle `task.Run(ctx)` et traite le résultat
4. Chaque worker envoie le résultat dans `resultChan`
5. Main goroutine collecte les résultats
6. À l'annulation du context → tous les workers s'arrêtent proprement

**Point clé** : la responsabilité du respect du contexte est **déléguée au callback** (task.Run). C'est au callback de respecter `ctx.Done()`. Le Processor n'a pas besoin de connaître les détails d'implémentation.

### Gestion des erreurs

Trois types d'erreurs :

- **ErrTaskFailed** : erreur métier (la tâche elle-même échoue)
- **ErrContextCanceled** : le context a été annulé
- **ErrTimeout** : timeout du context

```go
type TaskError struct {
    TaskID  string
    Err     error
}

func (e *TaskError) Is(target error) bool { /* ... */ }
```

## Exemples d'utilisation

### Créer une tâche

```go
// Tâche simple qui attend 150ms
task1 := NewTask("fetch-users", slowWork(150*time.Millisecond))

// Tâche qui échoue avec une erreur métier
task2 := NewTask("validate-email",
    failingWork(50*time.Millisecond, "invalid email format"))

// Callback personnalisé
task3 := NewTask("custom-op", func(ctx context.Context) error {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(200*time.Millisecond):
        // Faire du vrai travail ici
        return nil
    }
})
```

### Utilisation dans main.go

### Cas 1 : Traitement normal

```bash
go run . -scenario=normal
```

Traite 10 tâches avec 3 workers. Chaque tâche simule un travail métier réaliste (validation, appel API, etc.).

### Cas 2 : Gestion des erreurs

```bash
go run . -scenario=errors
```

Montre comment les erreurs métier sont propagées et agrégées.

### Cas 3 : Cancellation (Ctrl+C simulation)

```bash
go run . -scenario=cancel
```

Lance un traitement long, après 2 secondes interrompt et montre l'arrêt gracieux.

### Cas 4 : Timeout

```bash
go run . -scenario=timeout
```

Applique un timeout de 1 seconde sur des tâches de 2 secondes. Montre comment les workers s'arrêtent.

## Patterns clés à étudier

### Pattern 1 : Propagation du context

```go
// Le context "descend" dans la pile d'appels
func (p *Processor) Process(ctx context.Context, tasks []Task) error {
    for i := 0; i < p.workerCount; i++ {
        go func() {
            select {
            case <-ctx.Done():        // Écouter la cancellation
                return                 // Sortir gracieusement
            case task := <-p.taskChan: // Traiter la tâche
                // ...
            }
        }()
    }
}
```

### Pattern 2 : Fan-out / Fan-in

```
         [Main] envoie tasks
           ↓
    taskChan (fan-out)
         ↙  ↓  ↘
    [W1] [W2] [W3]  (workers)
         ↖  ↓  ↗
    resultChan (fan-in)
           ↓
      [Main] collecte
```

### Pattern 3 : Error wrapping idiomatique

```go
if err := doSomething(); err != nil {
    return fmt.Errorf("context: %w", err)  // wrapping
}

// À l'utilisation
if errors.Is(err, os.ErrNotExist) { /* ... */ }  // chercher un type spécifique
```

### Pattern 5 : Interface et encapsulation

```go
// Consumer défini l'interface
type Task interface {
    ID() string
    Run(ctx context.Context) error
}

// Producer fournit une implémentation (non-exportée)
type simpleTask struct { id string; run func(...) error }

// Constructor = adaptateur (comme http.HandlerFunc)
func NewTask(id string, run func(...) error) Task {
    return simpleTask{id, run}
}
```

**Avantage** : le Processor ne dépend que de l'interface, pas des détails d'implémentation.

## Points à noter

### ✅ Bonnes pratiques appliquées

- Context toujours premier paramètre (par convention)
- Pas d'erreurs silencieuses
- Pas de goroutines "orphelines"
- WaitGroup pour synchronisation fiable
- Timeouts explicites
- Nettoyage des canaux

### ⚠️ Pièges évités

- ❌ Goroutine lancée sans moyen de l'arrêter → Context.Done()
- ❌ Lectures bloquantes sur un canal → select avec context
- ❌ Fermeture d'un canal pendant des lectures actives → bien synchroniser
- ❌ Race conditions → design immutable des données partagées

## Comment exécuter

```bash
# Tout d'un coup
go run .

# Ou individuellement
go run . -scenario=normal
go run . -scenario=errors
go run . -scenario=cancel
go run . -scenario=timeout

# Avec race detector (important!)
go run -race .
```

## Structure du projet

```
concurrent-task-processor/
├── README.md          ← Ce fichier
├── go.mod
├── main.go            ← Scénarios de démonstration et entry point
├── processor.go       ← Logique du processeur (worker pool, coordination)
├── tasks.go           ← Implémentation Task et factories (slowWork, failingWork)
└── errors.go          ← Définitions des erreurs custom
```

## Pour approfondir après ce projet

### Patterns concurrence

1. **sync.Mutex / RWMutex** : partager état mutable de manière sécurisée
2. **errgroup** : pattern simplifié pour attendre plusieurs goroutines avec gestion d'erreurs
3. **select avec multiples canaux** : patterns avancés (timeout, priorités, etc.)
4. **context.Value()** : passer des valeurs structurées à travers le contexte

### Qualité & production

5. **Testing de concurrence** : `-race`, `-count=10000`, TestContext\* patterns
6. **pprof** : profiler goroutines, détecter fuites, analyser CPU/memory
7. **Observabilité** : logs structurés, traces distribuées (OpenTelemetry)

### Cas d'usage réels

- Batch processing (ETL, imports massifs)
- API gateways avec rate limiting
- Web scraping (crawlers multithread)
- Event processing (Kafka consumers)
- Database migrations
- File processing pipelines

## Exécution recommandée

1. Lire **processor.go** en premier (architecture worker pool, context propagation)
2. Lire **tasks.go** (implémentation Task, factories métier)
3. Lire **errors.go** (types d'erreurs custom)
4. Lancer les scénarios pour voir le concurrence en action
5. Modifier les callbacks pour expérimenter
6. Lancer avec `-race` pour vérifier l'absence de races conditions
