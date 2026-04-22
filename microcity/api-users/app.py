"""
api-users — FastAPI connecté à PostgreSQL via asyncpg.

Changement fondamental par rapport à la version in-memory :
  - "db" est résolu via le DNS Docker de backend-net → IP de microcity-db.
    Ce nom ne fonctionne QUE depuis un conteneur sur backend-net.
  - asyncpg implémente le protocole PostgreSQL en pur Python/Cython,
    sans dépendre de libpq (la bibliothèque C de PostgreSQL).
  - Pool de connexions : plusieurs requêtes HTTP simultanées partagent
    un pool de 2 à 10 connexions DB — on ne rouvre pas une connexion
    à chaque requête.

Flux de démarrage :
  lifespan() → create_pool() → CREATE TABLE IF NOT EXISTS → seed → serve
"""

import json
import logging
import os
import sys
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

import asyncpg
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Logging structuré JSON — cohérent avec api-orders et worker.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "service": "api-users", "msg": %(message)s}',
    stream=sys.stdout,
)
logger = logging.getLogger("api-users")

# ---------------------------------------------------------------------------
# Config depuis variables d'environnement injectées par docker-compose.
# "db" = nom du service PostgreSQL → résolu uniquement sur backend-net.
# ---------------------------------------------------------------------------
DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://microcity:microcity_secret@db:5432/microcity",
)

# Pool global — initialisé dans lifespan(), partagé par tous les handlers.
pool: asyncpg.Pool | None = None

# ---------------------------------------------------------------------------
# Métriques Prometheus — 4 Golden Signals : trafic, latence, erreurs
#
# Counter   : valeur strictement croissante depuis le démarrage du service.
#             Prometheus calcule le taux avec rate(metric[5m]).
#
# Histogram : distribue les observations dans des buckets de durée.
#             Permet le calcul de P50/P95/P99 en PromQL :
#             histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))
#
# Labels    : dimensions pour filtrer/agréger. Cardinalité BORNÉE obligatoire.
#             Ne jamais mettre un user_id, une IP, ou un token comme label
#             → explosion de cardinalité = OOM Prometheus.
# ---------------------------------------------------------------------------
HTTP_REQUESTS_TOTAL: Counter = Counter(
    "http_requests_total",
    "Nombre total de requêtes HTTP",
    ["method", "endpoint", "status"],
)
HTTP_REQUEST_DURATION: Histogram = Histogram(
    "http_request_duration_seconds",
    "Durée des requêtes HTTP en secondes",
    ["method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

# ---------------------------------------------------------------------------
# DDL — CREATE TABLE IF NOT EXISTS garantit l'idempotence.
# Chaque redémarrage du conteneur peut appeler ce DDL sans risque.
# ---------------------------------------------------------------------------
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
);
"""

SEED_DATA = [
    ("Alice", "alice@microcity.local"),
    ("Bob",   "bob@microcity.local"),
]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Gestion du cycle de vie FastAPI.

    Démarrage : crée le pool asyncpg, crée la table, seed si vide.
    Arrêt     : ferme proprement le pool (drain des connexions actives).

    asyncpg.create_pool() ouvre min_size=2 connexions immédiatement.
    Chaque handler acquiert une connexion via pool.acquire() et la
    rend automatiquement à la fin du bloc async with.

    Args:
        app: Instance FastAPI (injectée automatiquement).

    Yields:
        None: contrôle rendu au serveur ASGI entre démarrage et arrêt.
    """
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)

    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE)
        count: int = await conn.fetchval("SELECT COUNT(*) FROM users")
        if count == 0:
            await conn.executemany(
                "INSERT INTO users (name, email) VALUES ($1, $2)", SEED_DATA
            )

    logger.info(json.dumps({"event": "db_ready", "host": "db", "port": 5432}))
    yield
    await pool.close()
    logger.info(json.dumps({"event": "shutdown"}))


app = FastAPI(title="api-users", lifespan=lifespan)


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next: Any) -> Response:
    """Middleware HTTP — instrumente chaque requête pour Prometheus.

    Exclut /metrics de l'instrumentation (évite la récursivité).
    time.perf_counter() est plus précis que time.time() pour les durées courtes
    car il ne dépend pas de l'horloge système (NTP, corrections...).

    Args:
        request: Requête HTTP FastAPI entrante.
        call_next: Prochain handler ASGI dans la chaîne.

    Returns:
        Response HTTP avec métriques enregistrées.
    """
    if request.url.path == "/metrics":
        return await call_next(request)
    start = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start
    HTTP_REQUESTS_TOTAL.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code,
    ).inc()
    HTTP_REQUEST_DURATION.labels(
        method=request.method,
        endpoint=request.url.path,
    ).observe(duration)
    return response


class UserCreate(BaseModel):
    """Corps de la requête POST /users."""

    name: str
    email: str


@app.get("/health")
async def health() -> dict:
    """Health check utilisé par les load balancers et les scripts de test.

    Returns:
        dict: statut et nom du service.
    """
    return {"status": "ok", "service": "api-users"}


@app.get("/metrics")
async def metrics() -> Response:
    """Endpoint Prometheus — retourne les métriques au format text/plain.

    Prometheus scrape cet endpoint à intervalle régulier (scrape_interval).
    Le format est text/plain UTF-8 — chaque ligne = une observation.
    Exemple de sortie :
        http_requests_total{method="GET",endpoint="/users",status="200"} 42.0

    Returns:
        Response: Métriques en format Prometheus text exposition (MIME: text/plain).
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/users")
async def list_users() -> dict:
    """Retourne la liste complète des utilisateurs.

    Returns:
        dict: {"users": [{"id": int, "name": str, "email": str}, ...]}
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, email FROM users ORDER BY id")
    result = [{"id": r["id"], "name": r["name"], "email": r["email"]} for r in rows]
    logger.info(json.dumps({"event": "list_users", "count": len(result)}))
    return {"users": result}


@app.get("/users/{user_id}")
async def get_user(user_id: int) -> dict:
    """Retourne un utilisateur par son ID.

    Args:
        user_id: Identifiant entier de l'utilisateur.

    Returns:
        dict: {"id": int, "name": str, "email": str}

    Raises:
        HTTPException: 404 si l'utilisateur n'existe pas.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, email FROM users WHERE id = $1", user_id
        )
    if not row:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    return {"id": row["id"], "name": row["name"], "email": row["email"]}


@app.post("/users", status_code=201)
async def create_user(user: UserCreate) -> dict:
    """Crée un nouvel utilisateur.

    Args:
        user: Corps JSON avec name et email.

    Returns:
        dict: L'utilisateur créé avec son ID (status 201).

    Raises:
        HTTPException: 409 si l'email existe déjà (contrainte UNIQUE).
    """
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email",
                user.name,
                user.email,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(
                status_code=409, detail=f"Email {user.email} already exists"
            )
    logger.info(json.dumps({"event": "user_created", "id": row["id"]}))
    return {"id": row["id"], "name": row["name"], "email": row["email"]}
