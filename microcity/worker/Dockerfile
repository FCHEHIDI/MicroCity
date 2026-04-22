# =============================================================================
# worker/Dockerfile — multi-stage build Python 3.12
#
# Même pattern que api-users :
#   Stage 1 (builder) : installe les dépendances dans /install
#   Stage 2 (runtime) : image propre, copie uniquement /install + worker.py
#
# Pourquoi multi-stage ici aussi ?
#   pika installe quelques dépendances de build (wheel, setuptools...).
#   On ne veut pas ces outils dans l'image finale — surface d'attaque réduite.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1 — builder : installe les dépendances Python
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .

# --prefix=/install : installe dans un dossier isolé qu'on copiera au stage 2
# --no-cache-dir : pas de cache pip dans l'image → taille réduite
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ---------------------------------------------------------------------------
# Stage 2 — runtime : image finale légère
# ---------------------------------------------------------------------------
FROM python:3.12-slim

# Utilisateur non-root — le worker n'a besoin d'aucun privilège
RUN useradd --no-create-home --shell /bin/false worker

WORKDIR /app

# Copie les packages installés depuis le builder
COPY --from=builder /install /usr/local

# Copie le code applicatif
COPY worker.py .

# Assure que l'utilisateur worker possède son répertoire
RUN chown -R worker:worker /app

USER worker

# Pas de EXPOSE : le worker ne sert aucun port.
# Il initie des connexions sortantes vers RabbitMQ — jamais l'inverse.
CMD ["python", "worker.py"]
