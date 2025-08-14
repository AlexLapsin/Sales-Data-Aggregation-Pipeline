#!/usr/bin/env bash
# Purpose: Export TF_VAR_* from a .env file (does NOT run Terraform)
# Usage:
#   source scripts/export_tf_vars.sh
#   # or
#   source scripts/export_tf_vars.sh -e /full/path/to/.env

set -uo pipefail  # not using -e to avoid killing parent shell on 'return'

# --- helpers ---------------------------------------------------------------
_die() {
  # If sourced, 'return' instead of 'exit'
  if (return 0 2>/dev/null); then
    echo "ERROR: $*" >&2
    return 1
  else
    echo "ERROR: $*" >&2
    exit 1
  fi
}

# Detect if sourced (for info; we use _die() anyway)
_is_sourced=false
(return 0 2>/dev/null) && _is_sourced=true

# --- find repo root & default .env ----------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Try current dir first (works if script is in repo root)
if [[ -f "$SCRIPT_DIR/.env" ]]; then
  DEFAULT_ENV="$SCRIPT_DIR/.env"
# If script is in scripts/, try parent as repo root
elif [[ -f "$SCRIPT_DIR/../.env" ]]; then
  DEFAULT_ENV="$SCRIPT_DIR/../.env"
else
  # Fall back to current working directory
  DEFAULT_ENV="$PWD/.env"
fi

ENV_FILE="$DEFAULT_ENV"

# --- parse args ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -e|--env) ENV_FILE="$2"; shift 2 ;;
    *) _die "Unknown argument: $1" ;;
  esac
done

[[ -f "$ENV_FILE" ]] || _die ".env not found at: $ENV_FILE"

# --- load .env into current shell -----------------------------------------
# We want to set variables from .env without echoing them
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# --- map .env -> TF_VAR_* (adjust names if your variables.tf differs) ------
export TF_VAR_AWS_REGION="${AWS_DEFAULT_REGION:-}"
export TF_VAR_RAW_BUCKET="${S3_BUCKET:-}"

# processed bucket is optional
if [[ -n "${PROCESSED_BUCKET:-}" ]]; then
  export TF_VAR_PROCESSED_BUCKET="${PROCESSED_BUCKET}"
fi

export TF_VAR_DB_NAME="${RDS_DB:-}"
export TF_VAR_DB_USERNAME="${RDS_USER:-}"
export TF_VAR_DB_PASSWORD="${RDS_PASS:-}"
export TF_VAR_ALLOWED_CIDR="${ALLOWED_CIDR:-}"

# NEW: trusted principal for IAM trust policy
export TF_VAR_TRUSTED_PRINCIPAL_ARN="${TRUSTED_PRINCIPAL_ARN:-}"

# Optional tunables (export only if present in .env & variables.tf expects them)
[[ -n "${PROJECT_NAME:-}" ]]              && export TF_VAR_PROJECT_NAME="${PROJECT_NAME}"
[[ -n "${ENVIRONMENT:-}" ]]               && export TF_VAR_ENVIRONMENT="${ENVIRONMENT}"
[[ -n "${DB_INSTANCE_CLASS:-}" ]]         && export TF_VAR_DB_INSTANCE_CLASS="${DB_INSTANCE_CLASS}"
[[ -n "${DB_ALLOCATED_STORAGE:-}" ]]      && export TF_VAR_DB_ALLOCATED_STORAGE="${DB_ALLOCATED_STORAGE}"
[[ -n "${DB_ENGINE_VERSION:-}" ]]         && export TF_VAR_DB_ENGINE_VERSION="${DB_ENGINE_VERSION}"
[[ -n "${DB_BACKUP_RETENTION_DAYS:-}" ]]  && export TF_VAR_DB_BACKUP_RETENTION_DAYS="${DB_BACKUP_RETENTION_DAYS}"
[[ -n "${PUBLICLY_ACCESSIBLE:-}" ]]       && export TF_VAR_PUBLICLY_ACCESSIBLE="${PUBLICLY_ACCESSIBLE}"

# --- protect AWS credential chain -----------------------------------------
# If you prefer using an AWS profile for Terraform, keep it working:
# - If AWS_PROFILE is set, clear inline creds so the profile wins.
# - If .env had empty creds, clear them so they don't override a working chain.
if [[ -n "${AWS_PROFILE:-}" ]]; then
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
fi
[[ -z "${AWS_ACCESS_KEY_ID:-}" ]]     && unset AWS_ACCESS_KEY_ID
[[ -z "${AWS_SECRET_ACCESS_KEY:-}" ]] && unset AWS_SECRET_ACCESS_KEY
[[ -z "${AWS_SESSION_TOKEN:-}" ]]     && unset AWS_SESSION_TOKEN

# --- sanity checks (don’t echo secrets) ------------------------------------
_req=(AWS_DEFAULT_REGION S3_BUCKET RDS_DB RDS_USER RDS_PASS ALLOWED_CIDR)
for v in "${_req[@]}"; do
  [[ -n "${!v:-}" ]] || _die "$v missing in $ENV_FILE"
done

echo "TF_VAR_* exported from: $ENV_FILE"
echo "Next:  cd infra && terraform plan"
# If sourced, 'return 0' so shell continues; if executed, exit 0
$_is_sourced && return 0 || exit 0
