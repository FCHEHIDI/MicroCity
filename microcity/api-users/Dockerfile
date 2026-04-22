# =============================================================================
# MicroCity — api-users/Dockerfile
#
# Build multi-stage :
#   Stage 1 "builder" : installe les dépendances dans un environnement isolé
#   Stage 2 "runtime" : copie uniquement ce qui est nécessaire à l'exécution
#
# Pourquoi multi-stage ?
#   - L'image finale ne contient pas pip, les headers de compilation, les caches
#   - Taille réduite = surface d'attaque réduite + pull plus rapide
#   - Pattern standard en production conteneurisée
# =============================================================================

# --- Stage 1 : builder -------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /build

# Copier les dépendances en premier — Docker met ce layer en cache.
# Si requirements.txt ne change pas, ce layer n'est pas reconstruit
# même si app.py change. C'est la règle du cache Docker : copier ce qui
# change le moins souvent en premier.
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# --- Stage 2 : runtime -------------------------------------------------------
FROM python:3.12-slim AS runtime

# Utilisateur non-root — bonne pratique de sécurité.
# Un conteneur qui tourne en root peut, en cas de faille, obtenir
# des privilèges sur l'hôte via des escape techniques.
RUN useradd --no-create-home --shell /bin/false appuser

WORKDIR /app

# Copier les dépendances installées depuis le stage builder
COPY --from=builder /install /usr/local

# Copier le code applicatif
COPY app.py .

# Changer le propriétaire des fichiers
RUN chown -R appuser:appuser /app

USER appuser

# Port sur lequel Uvicorn écoute à l'intérieur du conteneur.
# Ce port n'est pas exposé vers l'hôte — seul Traefik y accède via backend-net.
EXPOSE 8000

# Commande de démarrage.
# --host 0.0.0.0 : écoute sur toutes les interfaces du conteneur (nécessaire
#   pour être joignable depuis d'autres conteneurs sur le même réseau).
# --workers 1 : un seul worker pour ce tuto (pas de besoin de concurrence).
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
