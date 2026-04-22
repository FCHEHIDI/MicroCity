"""
worker.py — Consommateur RabbitMQ pour MicroCity.

Ce worker illustre plusieurs invariants réseau Docker :
  - Il résout "queue" via le DNS Docker de backend-net.
    "queue" n'est pas une IP codée en dur — c'est le nom du service
    dans docker-compose. Docker injecte une entrée DNS dans /etc/resolv.conf
    du conteneur : "queue" → IP assignée sur backend-net.
  - Il n'est PAS sur frontend-net → aucun trafic HTTP entrant ne peut
    l'atteindre. Il est strictement consommateur.
  - La connexion AMQP part de backend-net, pas depuis l'hôte.

Flux de données :
  api-orders → publish("orders") → RabbitMQ(backend-net)
                                         ↓ consume
                                      worker → log structuré JSON
"""

import json
import logging
import os
import sys
import time
from typing import Any

import pika
import pika.exceptions
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from prometheus_client import Counter, start_http_server


# ---------------------------------------------------------------------------
# Logging structuré JSON — cohérent avec api-users et api-orders.
# En production ces logs sont collectés par un agent (Fluent Bit, etc.)
# et indexés par champ (level, service, msg...).
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "service": "worker", "msg": %(message)s}',
    stream=sys.stdout,
)
logger = logging.getLogger("worker")


# ---------------------------------------------------------------------------
# Configuration via variables d'environnement.
# Jamais de valeurs codées en dur — la config vient de docker-compose.yaml.
# ---------------------------------------------------------------------------
RABBITMQ_URL: str = os.environ.get(
    "RABBITMQ_URL", "amqp://microcity:microcity_secret@queue:5672/"
)
QUEUE_NAME: str = os.environ.get("QUEUE_NAME", "orders")

# Retry parameters — RabbitMQ peut prendre quelques secondes à démarrer
# même après que son healthcheck est vert.
CONNECT_RETRIES: int = 10
CONNECT_DELAY: float = 3.0

# ---------------------------------------------------------------------------
# Métriques Prometheus
#
# start_http_server(8002) démarre un serveur HTTP minimal en thread daemon.
# Pas de FastAPI / Flask ici — juste le serveur intégré de prometheus_client.
# Prometheus scrape http://worker:8002/metrics via observability-net.
#
# messages_consumed : s'incrémente à chaque message traité avec succès (acked).
# messages_failed   : s'incrémente pour chaque message nacké (decode_error, etc.).
# Label queue       : prépare l'extension future (plusieurs queues / workers).
# ---------------------------------------------------------------------------
MESSAGES_CONSUMED: Counter = Counter(
    "worker_messages_consumed_total",
    "Total messages successfully consumed and acked",
    ["queue"],
)
MESSAGES_FAILED: Counter = Counter(
    "worker_messages_failed_total",
    "Total messages failed to process and nacked",
    ["queue", "reason"],
)


def on_message(
    channel: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: bytes,
) -> None:
    """Callback appelé par pika pour chaque message reçu.

    Args:
        channel: Canal AMQP actif.
        method: Métadonnées de livraison (delivery_tag, routing_key...).
        properties: Propriétés du message AMQP (content_type, headers...).
        body: Corps du message en bytes.
    """
    try:
        data: Any = json.loads(body)
        logger.info(json.dumps({"event": "message_received", "queue": QUEUE_NAME, "payload": data}))

        # Ici on traiterait la commande :
        #   - écrire en base via asyncpg
        #   - déclencher un email
        #   - publier une métrique OpenTelemetry
        # Pour l'instant on ack immédiatement (message consommé avec succès).

        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(json.dumps({"event": "message_acked", "delivery_tag": method.delivery_tag}))
        MESSAGES_CONSUMED.labels(queue=QUEUE_NAME).inc()

    except json.JSONDecodeError as exc:
        logger.error(json.dumps({"event": "decode_error", "error": str(exc), "body": body.decode(errors="replace")}))
        # nack sans requeue — message malformé, on l'envoie en dead-letter
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        MESSAGES_FAILED.labels(queue=QUEUE_NAME, reason="decode_error").inc()


def connect_with_retry(url: str, retries: int = CONNECT_RETRIES, delay: float = CONNECT_DELAY) -> pika.BlockingConnection:
    """Tente de se connecter à RabbitMQ avec retries exponentiels.

    Docker healthcheck dit "service healthy" quand RabbitMQ accepte un ping,
    mais le broker peut encore refuser des connexions AMQP pendant 1-2s.
    Ce retry loop absorbe cette fenêtre.

    Args:
        url: URL AMQP complète (ex: amqp://user:pass@host:5672/).
        retries: Nombre maximum de tentatives.
        delay: Délai en secondes entre chaque tentative.

    Returns:
        Une connexion pika.BlockingConnection active.

    Raises:
        RuntimeError: Si toutes les tentatives échouent.
    """
    for attempt in range(1, retries + 1):
        try:
            params = pika.URLParameters(url)
            connection = pika.BlockingConnection(params)
            logger.info(json.dumps({"event": "connected", "attempt": attempt, "url": url.split("@")[-1]}))
            return connection
        except pika.exceptions.AMQPConnectionError as exc:
            logger.warning(json.dumps({"event": "connect_failed", "attempt": attempt, "max": retries, "error": str(exc)}))
            if attempt < retries:
                time.sleep(delay)

    raise RuntimeError(f"Could not connect to RabbitMQ after {retries} attempts")


def main() -> None:
    """Point d'entrée principal — connexion + boucle de consommation.

    Déclare la queue avant de consommer : si la queue n'existe pas encore
    (ex: api-orders n'a pas encore publié), elle est créée ici.
    durable=True → la queue survit à un restart de RabbitMQ.
    """
    # Expose les métriques Prometheus sur le port 8002.
    # start_http_server() démarre un thread HTTP daemon en arrière-plan.
    # Le thread s'arrête automatiquement quand le processus principal termine.
    start_http_server(8002)
    logger.info(json.dumps({"event": "metrics_server_started", "port": 8002}))

    connection = connect_with_retry(RABBITMQ_URL)
    channel = connection.channel()

    # Déclaration idempotente : si la queue existe déjà avec les mêmes params,
    # aucune erreur. Si les params diffèrent → exception (conflit).
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # prefetch_count=1 : le worker ne reçoit un nouveau message que quand
    # il a ack le précédent → distribution équitable si plusieurs workers tournent.
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    logger.info(json.dumps({"event": "listening", "queue": QUEUE_NAME}))

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info(json.dumps({"event": "shutdown", "reason": "KeyboardInterrupt"}))
        channel.stop_consuming()

    connection.close()


if __name__ == "__main__":
    main()
