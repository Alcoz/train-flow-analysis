#!/usr/bin/env bash
# Runs on the server, from the repository, after `docker compose up`: waits for
# Dagster to start and load the code location, then reports on Discord.
#
#   post_deploy_check.sh               check the deployment and report
#   post_deploy_check.sh fail <step>   only report that a deploy step failed

set -uo pipefail

DAGSTER_URL="${DAGSTER_URL:-http://localhost:3000}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"
SERVICES="dagster_postgres train_orchestrator dagster_webserver dagster_daemon"
WEBHOOK_URL="$(grep -E '^DISCORD_WEBHOOK_URL=' .env 2>/dev/null | cut -d= -f2- | tr -d "\"'")"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g' | tr '\n' ' '
}

COMMIT="$(json_escape "$(git log -1 --format='%h %s')")"

# $1 is already JSON-safe, with literal \n for line breaks
notify() {
  printf '%b\n' "$1"
  if [ -n "$WEBHOOK_URL" ]; then
    curl -sS -m 10 -H 'Content-Type: application/json' \
      -d "{\"content\": \"$1\"}" "$WEBHOOK_URL" >/dev/null \
      || echo "Envoi Discord impossible"
  fi
}

fail() {
  notify "🔴 **Déploiement KO** : \`$COMMIT\`\n$(json_escape "$1")"
  exit 1
}

if [ "${1:-}" = "fail" ]; then
  fail "Échec à l'étape : ${2:-inconnue}"
fi

query='{"query":"{ workspaceOrError { ... on Workspace { locationEntries { locationOrLoadError { __typename ... on RepositoryLocation { repositories { schedules { scheduleState { status } } } } } } } } }"}'
deadline=$((SECONDS + TIMEOUT_SECONDS))

while :; do
  problem=""
  for service in $SERVICES; do
    state="$(docker inspect -f '{{.State.Status}}' "$service" 2>/dev/null || echo absent)"
    [ "$state" = running ] || problem="conteneur $service : $state"
  done

  if [ -z "$problem" ]; then
    workspace="$(curl -sS -m 5 -H 'Content-Type: application/json' -d "$query" "$DAGSTER_URL/graphql" 2>/dev/null)"
    case "$workspace" in
      *RepositoryLocation*) break ;;
      *PythonError*) problem="le code location ne charge pas (voir l'onglet Deployment de Dagster)" ;;
      *) problem="le webserver Dagster ne répond pas sur $DAGSTER_URL" ;;
    esac
  fi

  [ "$SECONDS" -ge "$deadline" ] && fail "Après ${TIMEOUT_SECONDS}s, $problem"
  sleep 5
done

# Schedules are turned on by hand: a fresh database starts with all of them off.
running="$(printf '%s' "$workspace" | grep -o '"status":"RUNNING"' | wc -l | tr -d ' ')"
total="$(printf '%s' "$workspace" | grep -o '"status":"[A-Z]*"' | wc -l | tr -d ' ')"
notify "✅ **Déploiement OK** : \`$COMMIT\`\nDagster a démarré et chargé le code. Schedules actifs : $running/$total"
