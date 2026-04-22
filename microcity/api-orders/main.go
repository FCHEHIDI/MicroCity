// MicroCity — api-orders (Go / net/http)
//
// Phase 3 : connexions réelles à PostgreSQL et RabbitMQ.
//
// Dépendances réseau (résolues via DNS Docker sur backend-net) :
//   - db:5432    → PostgreSQL (lib/pq driver, database/sql interface)
//   - queue:5672 → RabbitMQ  (amqp091-go)
//
// Invariant de conception :
//   La publication AMQP est fire-and-forget. Si RabbitMQ est indisponible,
//   la commande est quand même créée en DB — le bus de messages ne bloque
//   pas le chemin critique de création de commande.

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq" // driver PostgreSQL pour database/sql — import side-effect
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// -----------------------------------------------------------------------------
// Modèles
// -----------------------------------------------------------------------------

// ─── Config ──────────────────────────────────────────────────────────────────

var (
	databaseURL = getenv("DATABASE_URL", "postgres://microcity:microcity_secret@db:5432/microcity?sslmode=disable")
	rabbitmqURL = getenv("RABBITMQ_URL", "amqp://microcity:microcity_secret@queue:5672/")
	queueName   = getenv("QUEUE_NAME", "orders")
	listenAddr  = getenv("LISTEN_ADDR", ":8001")
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─── Métriques Prometheus ───────────────────────────────────────────────────────────
//
// promauto.New* : enregistre automatiquement dans le registry Prometheus
// par défaut. promhttp.Handler() les expose en text/plain sur GET /metrics.
//
// ordersCreatedTotal  : counter — s'incrémente à chaque POST /orders réussi.
//                       Utile pour le taux de création : rate(orders_created_total[5m])
// httpRequestsTotal   : counter vec — trafic par méthode + chemin + status.
// httpRequestDuration : histogram — distribution des latences par endpoint.

var (
	ordersCreatedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "orders_created_total",
		Help: "Total orders successfully created via POST /orders",
	})
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests",
	}, []string{"method", "path", "status"})
	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration in seconds",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"method", "path"})
)

// responseWriter capture le status code écrit par les handlers.
// http.ResponseWriter ne fournit pas d'accès au status après WriteHeader —
// on doit wrapper pour l'intercepter.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// metricsMiddleware enregistre durée et count de chaque requête HTTP.
// /metrics est exclu pour éviter la récursivité (scrape Prometheus lui-même).
func (a *App) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		duration := time.Since(start).Seconds()
		httpRequestsTotal.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(rw.status)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, r.URL.Path).Observe(duration)
	})
}

// ─── Domaine ─────────────────────────────────────────────────────────────────

// Order représente une commande persistée en base.
type Order struct {
	ID     int    `json:"id"`
	UserID int    `json:"user_id"`
	Item   string `json:"item"`
	Status string `json:"status"`
}

// OrderCreate est le payload attendu pour POST /orders.
type OrderCreate struct {
	UserID int    `json:"user_id"`
	Item   string `json:"item"`
}

// ─── App ─────────────────────────────────────────────────────────────────────

// App contient les deux connexions persistantes :
//   - db     : pool de connexions PostgreSQL (database/sql gère le pool)
//   - amqpCh : canal AMQP ouvert vers RabbitMQ
type App struct {
	db       *sql.DB
	amqpConn *amqp.Connection
	amqpCh   *amqp.Channel
}

// initDB ouvre le pool PostgreSQL et tente de se connecter avec retry.
//
// database/sql.Open() ne crée pas de connexion immédiatement — il valide
// juste le driver et le DSN. C'est Ping() qui initie la première connexion.
// Le healthcheck Docker garantit que PostgreSQL est prêt, mais on garde
// le retry comme filet de sécurité pour la fenêtre de démarrage.
func initDB(url string) (*sql.DB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	for i := range 10 {
		if err = db.Ping(); err == nil {
			return db, nil
		}
		slog.Warn("db not ready", "attempt", i+1, "err", err)
		time.Sleep(2 * time.Second)
	}
	return nil, err
}

// initAMQP ouvre une connexion AMQP et un canal vers RabbitMQ.
//
// Le canal déclare la queue comme durable (survivant aux restarts RabbitMQ).
// La déclaration est idempotente : si la queue existe déjà avec les mêmes
// paramètres, aucune erreur. Le worker.py déclare la même queue — le premier
// arrivé la crée, les suivants la trouvent déjà là.
func initAMQP(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	// durable=true   : queue survit à un restart RabbitMQ
	// autoDelete=false : queue reste même si tous les consommateurs se déconnectent
	// exclusive=false  : plusieurs connexions peuvent utiliser cette queue
	// noWait=false     : attend la confirmation du broker
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, err
	}
	return conn, ch, nil
}

// ─── DDL + Seed ──────────────────────────────────────────────────────────────

const createTable = `
CREATE TABLE IF NOT EXISTS orders (
    id      SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    item    TEXT NOT NULL,
    status  TEXT NOT NULL DEFAULT 'pending'
);`

type seedRow struct {
	userID       int
	item, status string
}

var seedOrders = []seedRow{
	{1, "cafe", "delivered"},
	{2, "croissant", "pending"},
}

// ─── Handlers ────────────────────────────────────────────────────────────────

// writeJSON sérialise v en JSON et l'écrit avec le status code donné.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("writeJSON encode error", "err", err)
	}
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "api-orders"})
}

func (a *App) handleListOrders(w http.ResponseWriter, r *http.Request) {
	rows, err := a.db.QueryContext(r.Context(), "SELECT id, user_id, item, status FROM orders ORDER BY id")
	if err != nil {
		slog.Error("list orders", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	orders := make([]Order, 0)
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.ID, &o.UserID, &o.Item, &o.Status); err != nil {
			slog.Error("scan order", "err", err)
			continue
		}
		orders = append(orders, o)
	}
	slog.Info("list orders", "count", len(orders))
	writeJSON(w, http.StatusOK, map[string]any{"orders": orders})
}

func (a *App) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	// Go 1.22 pattern routing : r.PathValue("id") extrait le segment {id}
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	var o Order
	err = a.db.QueryRowContext(r.Context(),
		"SELECT id, user_id, item, status FROM orders WHERE id = $1", id,
	).Scan(&o.ID, &o.UserID, &o.Item, &o.Status)
	if err == sql.ErrNoRows {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		slog.Error("get order", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, o)
}

func (a *App) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var input OrderCreate
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if input.UserID == 0 || input.Item == "" {
		http.Error(w, "user_id and item required", http.StatusBadRequest)
		return
	}

	var o Order
	err := a.db.QueryRowContext(r.Context(),
		"INSERT INTO orders (user_id, item, status) VALUES ($1, $2, 'pending') RETURNING id, user_id, item, status",
		input.UserID, input.Item,
	).Scan(&o.ID, &o.UserID, &o.Item, &o.Status)
	if err != nil {
		slog.Error("create order", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Publication AMQP — fire and forget.
	// La commande est créée en DB même si cette étape échoue.
	if err := a.publish(o); err != nil {
		slog.Warn("amqp publish failed", "order_id", o.ID, "err", err)
	}

	slog.Info("order created", "id", o.ID, "user_id", o.UserID, "item", o.Item)
	ordersCreatedTotal.Inc()
	writeJSON(w, http.StatusCreated, o)
}

// publish sérialise l'ordre en JSON et le publie sur la queue AMQP.
//
// Exchange "" = default exchange (direct routing).
// La routing key est le nom de la queue — le message arrive directement
// dans la queue "orders" sans passer par un exchange nommé.
// DeliveryMode=Persistent : le message survit à un restart RabbitMQ.
func (a *App) publish(o Order) error {
	body, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return a.amqpCh.PublishWithContext(
		context.Background(),
		"",        // exchange — "" = default direct exchange
		queueName, // routing key
		false,     // mandatory : pas d'erreur si aucun consommateur
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

// ─── Routes ──────────────────────────────────────────────────────────────────

func (a *App) routes() *http.ServeMux {
	mux := http.NewServeMux()
	// Go 1.22 : méthode + pattern dans la même chaîne
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.HandleFunc("GET /orders", a.handleListOrders)
	mux.HandleFunc("GET /orders/{id}", a.handleGetOrder)
	mux.HandleFunc("POST /orders", a.handleCreateOrder)
	// promhttp.Handler() expose les métriques Go runtime + custom au format text/plain.
	// Scrapeé directement par Prometheus via observability-net : pas de passage par Traefik.
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	// Logger JSON structuré (cohérent avec api-users et worker)
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// 1. PostgreSQL
	db, err := initDB(databaseURL)
	if err != nil {
		slog.Error("db init failed", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	if _, err := db.Exec(createTable); err != nil {
		slog.Error("create table", "err", err)
		os.Exit(1)
	}
	var count int
	db.QueryRow("SELECT COUNT(*) FROM orders").Scan(&count)
	if count == 0 {
		for _, s := range seedOrders {
			db.Exec("INSERT INTO orders (user_id, item, status) VALUES ($1, $2, $3)", s.userID, s.item, s.status)
		}
	}
	slog.Info("db ready", "host", "db", "port", 5432)

	// 2. RabbitMQ
	amqpConn, amqpCh, err := initAMQP(rabbitmqURL)
	if err != nil {
		slog.Error("amqp init failed", "err", err)
		os.Exit(1)
	}
	defer amqpConn.Close()
	defer amqpCh.Close()
	slog.Info("amqp ready", "host", "queue", "port", 5672)

	app := &App{db: db, amqpConn: amqpConn, amqpCh: amqpCh}
	slog.Info("api-orders listening", "addr", listenAddr)
	// metricsMiddleware enveloppe le mux pour instrumenter chaque requête.
	if err := http.ListenAndServe(listenAddr, app.metricsMiddleware(app.routes())); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
