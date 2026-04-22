# =============================================================================
# MicroCity — api-orders/Dockerfile
#
# Go permet un build encore plus propre que Python : le binaire compilé
# est un fichier unique, autonome, sans dépendances runtime.
# L'image finale peut donc utiliser "scratch" (image vide) ou "distroless".
# On utilise distroless pour garder les certificats TLS et les fuseaux horaires.
#
# Build multi-stage :
#   Stage 1 "builder" : compile le binaire avec le toolchain Go complet
#   Stage 2 "runtime" : copie uniquement le binaire compilé — rien d'autre
# =============================================================================

# --- Stage 1 : compilation ---------------------------------------------------
FROM golang:1.22-alpine AS builder

WORKDIR /build

# Copier go.mod d'abord pour mettre les dépendances en cache.
# Ici on n'a pas de dépendances externes (stdlib uniquement),
# mais le pattern est le même quand on en ajoute.
# Copier go.mod + code ensemble — go mod tidy analyse les imports pour
# télécharger les dépendances ET générer go.sum en une seule étape.
# Avantage : pas besoin de go.sum pré-généré sur l'hôte.
# Contrepartie : le cache Docker de cette couche invalide si go.mod OU main.go change.
COPY go.mod .
COPY main.go .

# go mod tidy : télécharge github.com/lib/pq et github.com/rabbitmq/amqp091-go,
# génère go.sum, puis compile en binaire statique.
# CGO_ENABLED=0 : binaire 100% statique → compatible distroless/static.
# -ldflags="-s -w" : supprime symboles de debug → binaire ~30% plus petit.
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o api-orders .

# --- Stage 2 : runtime -------------------------------------------------------
# gcr.io/distroless/static : image sans shell, sans package manager, sans rien.
# Contient uniquement : libc statique, certificats TLS, fichiers timezone.
# Surface d'attaque minimale.
FROM gcr.io/distroless/static:nonroot

WORKDIR /app

# Copier uniquement le binaire compilé
COPY --from=builder /build/api-orders .

# L'image distroless:nonroot démarre déjà en utilisateur non-root (uid 65532)
# Pas besoin de RUN useradd

EXPOSE 8001

# Pas de shell dans distroless — la commande doit être en forme "exec" (tableau JSON)
CMD ["/app/api-orders"]
