#!/usr/bin/env bash
# One-time (and re-runnable) schema bootstrap, run AFTER `terraform apply`.
# Terraform provisions the Redshift Serverless namespace/workgroup itself, but
# has no native resource for arbitrary table/procedure DDL, so that step is
# done here via the Redshift Data API instead.
#
# Usage: ./run_sql.sh <workgroup-name> <database-name> <secret-arn>
set -euo pipefail

WORKGROUP="${1:?workgroup name required}"
DATABASE="${2:?database name required}"
SECRET_ARN="${3:?secret arn required}"

run_statement() {
  local file="$1"
  echo "Running $file ..."
  local statement_id
  statement_id=$(aws redshift-data execute-statement \
    --workgroup-name "$WORKGROUP" \
    --database "$DATABASE" \
    --secret-arn "$SECRET_ARN" \
    --sql "file://$file" \
    --query "Id" --output text)

  while true; do
    status=$(aws redshift-data describe-statement --id "$statement_id" --query "Status" --output text)
    case "$status" in
      FINISHED) echo "  done"; break ;;
      FAILED|ABORTED) echo "  FAILED: $(aws redshift-data describe-statement --id "$statement_id" --query 'Error' --output text)"; exit 1 ;;
      *) sleep 2 ;;
    esac
  done
}

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../sql" && pwd)"
run_statement "$DIR/01_staging_ddl.sql"
run_statement "$DIR/02_dimensional_ddl.sql"
run_statement "$DIR/03_transform.sql"

echo "Schema bootstrap complete."
