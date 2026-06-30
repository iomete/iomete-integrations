#!/usr/bin/env bash
# Reliably run the dbt-iomete integration + functional suites.
#
# Ensures the required IOMETE resources exist and the compute is healthy
# (scripts/ci/provision.py), then runs both suites, always running
# both even if the first fails, and reports a single aggregated status. This is
# the one entrypoint used both locally and in CI.
#
# Configuration comes from DBT_IOMETE_* env vars (see tests/README.md). Locally a
# dbt-iomete/.env file is loaded automatically; in CI the variables are provided
# by the workflow.
#
# Usage:
#   scripts/ci/run-integration-tests.sh                  # provision + integration + functional
#   SUITES="integration" scripts/ci/run-integration-tests.sh   # subset: integration | functional
#   SKIP_PROVISION=1 scripts/ci/run-integration-tests.sh        # skip provisioning (and teardown)
#   KEEP_RESOURCES=1 scripts/ci/run-integration-tests.sh        # provision but do not tear down
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$DBT_DIR"

SUITES="${SUITES:-integration functional}"
PYTHON_BIN="${PYTHON_BIN:-python}"
STATE_FILE="${STATE_FILE:-$SCRIPT_DIR/.provision-state.json}"

if [[ "${SKIP_PROVISION:-0}" != "1" ]]; then
  echo "[run-integration-tests] provisioning"
  # Arrange teardown first, so an interrupted provision still gets cleaned up
  # (provision writes its state file incrementally). KEEP_RESOURCES=1 opts out.
  if [[ "${KEEP_RESOURCES:-0}" != "1" ]]; then
    trap '"$PYTHON_BIN" "$SCRIPT_DIR/teardown.py" --state-file "$STATE_FILE" || true' EXIT
  fi
  "$PYTHON_BIN" "$SCRIPT_DIR/provision.py" provision --state-file "$STATE_FILE"

  # Provision wrote the temp user's credentials to dbt-iomete/.env.test; the
  # suites pick them up via pytest-dotenv (see tox.ini), so no export is needed.
  echo "[run-integration-tests] healthcheck"
  "$PYTHON_BIN" "$SCRIPT_DIR/provision.py" healthcheck --state-file "$STATE_FILE"
else
  echo "[run-integration-tests] SKIP_PROVISION=1 — skipping provisioning and teardown"
fi

declare -i failures=0
declare -a failed_suites=()

run_suite() {
  local name="$1" tox_env="$2"
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    echo "::group::${name} tests"
  else
    echo "===== ${name} tests ====="
  fi
  if tox -e "$tox_env"; then
    echo "[run-integration-tests] ${name}: PASS"
  else
    echo "[run-integration-tests] ${name}: FAIL"
    failures+=1
    failed_suites+=("$name")
  fi
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::endgroup::"; fi
}

for suite in $SUITES; do
  case "$suite" in
    integration) run_suite "integration" "integration-iomete" ;;
    functional)  run_suite "functional"  "functional" ;;
    *) echo "[run-integration-tests] unknown suite: $suite" >&2; exit 2 ;;
  esac
done

if (( failures > 0 )); then
  echo "[run-integration-tests] FAILED suites: ${failed_suites[*]}" >&2
  exit 1
fi
echo "[run-integration-tests] all suites passed"
