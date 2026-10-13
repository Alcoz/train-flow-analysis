#!/usr/bin/env bash

set -euo pipefail

# Report any failing step on Discord; the check script lives in the repo on the server.
step="récupération du code"
report_failure() {
  ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" \
    "cd $REMOTE_PATH && bash post_deploy_check.sh fail '$step'" || true
}
trap report_failure ERR

echo "==> Vérification / initialisation du repository distant"
ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" bash -s <<EOF
set -euo pipefail
if [ -d "$REMOTE_PATH/.git" ]; then
  echo "Repo déjà présent, pull..."
  cd "$REMOTE_PATH"
  git pull
else
  echo "Repo absent, clonage..."
  mkdir -p "$(dirname "$REMOTE_PATH")"
  git clone $REPO_URL
fi
EOF

step="build des images"
echo "Build Docker image"
ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" "cd $REMOTE_PATH && docker compose --env-file .env build"

step="démarrage des conteneurs"
echo "Deploy Docker containers"
ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" "cd $REMOTE_PATH && docker compose --env-file .env up -d"

step="nettoyage Docker"
echo "==> Pruning des images Docker inutilisées"
ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" "docker image prune -f"

echo "==> Pruning du cache de build Docker de plus de 7 jours"
ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" "docker builder prune -f --filter until=168h"

# The check reports its own failure, so the ERR trap must not report it twice.
trap - ERR
echo "==> Vérification du déploiement"
ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" "cd $REMOTE_PATH && bash post_deploy_check.sh"
