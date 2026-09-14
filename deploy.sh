set -euo pipefail

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

echo "Build Docker image"
ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" "cd $REMOTE_PATH && docker compose --env-file .env build"

echo "Deploy Docker containers"
ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" "cd $REMOTE_PATH && docker compose --env-file .env up -d"

echo "==> Pruning des images Docker inutilisées"
ssh -i "$SSH_KEY_PATH" "$SSH_USER@$SSH_HOST" "docker image prune -f"
