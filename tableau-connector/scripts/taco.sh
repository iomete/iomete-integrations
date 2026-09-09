#!/usr/bin/env bash
#
# Validate or package the IOMETE Tableau connector.
#
#   ./scripts/taco.sh validate
#   ./scripts/taco.sh build [dest-dir]
#   ./scripts/taco.sh sign <taco>     # needs TACO_KEYSTORE and
#   ./scripts/taco.sh verify <taco>   # TACO_KEYSTORE_PASSWORD

set -euo pipefail

SDK_TAG="${CONNECTOR_SDK_TAG:-tableau-2024.2}"
CACHE_DIR="${TACO_CACHE_DIR:-${HOME}/.cache/iomete-tableau-connector}"
SDK_DIR="${CACHE_DIR}/connector-plugin-sdk-${SDK_TAG}"
VENV_DIR="${CACHE_DIR}/venv-${SDK_TAG}"

connector_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
connector_dir="${connector_root}/connector"

setup() {
  # An interrupted clone leaves the directory behind, so test what it should hold.
  if [ ! -d "${SDK_DIR}/connector-packager" ]; then
    echo "Cloning Connector SDK at ${SDK_TAG}..."
    rm -rf "${SDK_DIR}"
    git clone --quiet --depth 1 --branch "${SDK_TAG}" \
      https://github.com/tableau/connector-plugin-sdk.git "${SDK_DIR}"
  fi

  [ -x "${VENV_DIR}/bin/python" ] || python3 -m venv "${VENV_DIR}"

  "${VENV_DIR}/bin/python" -c 'import connector_packager' 2>/dev/null ||
    "${VENV_DIR}/bin/python" -m pip install --quiet -e "${SDK_DIR}/connector-packager"
}

# The packager returns 0 whether validation passes or fails, so its output is
# the only reliable signal.
run_packager() {
  local log_dir output
  log_dir="$(mktemp -d)"

  # Schemas resolve relative to the packager's own directory.
  output="$("${VENV_DIR}/bin/python" -m connector_packager.package \
    --log "${log_dir}" "$@" 2>&1)" || true

  echo "${output}"

  if ! grep -q 'Validation succeeded.' <<<"${output}"; then
    echo
    echo "Validation failed. Full log: ${log_dir}/packaging_logs.txt" >&2
    return 1
  fi
}

validate() {
  setup
  cd "${SDK_DIR}/connector-packager"
  run_packager --validate-only "${connector_dir}"
}

# Global: the EXIT trap runs after the function returns, where a local would be
# unset under `set -u`.
staging=""

build() {
  local dest
  dest="$(mkdir -p "${1:-${connector_root}/build}" && cd "${1:-${connector_root}/build}" && pwd)"

  setup

  # Packaging rewrites manifest.xml in place and restores it afterwards, so it
  # runs against a copy to keep an interrupted build out of the repo.
  staging="$(mktemp -d)"
  trap 'rm -rf "${staging}"' EXIT
  cp -R "${connector_dir}" "${staging}/connector"

  cd "${SDK_DIR}/connector-packager"
  run_packager --dest "${dest}" "${staging}/connector"

  echo
  ls -l "${dest}"/*.taco
}

sign() {
  local taco="${1:?usage: taco.sh sign <taco>}"

  : "${TACO_KEYSTORE:?set TACO_KEYSTORE to the self-signing keystore}"
  : "${TACO_KEYSTORE_PASSWORD:?set TACO_KEYSTORE_PASSWORD}"

  jarsigner \
    -keystore "${TACO_KEYSTORE}" \
    -storepass:env TACO_KEYSTORE_PASSWORD \
    "${taco}" "${TACO_SIGNING_ALIAS:-iomete-taco}"

  verify "${taco}"
}

verify() {
  local taco="${1:?usage: taco.sh verify <taco>}" output status=0

  : "${TACO_KEYSTORE:?set TACO_KEYSTORE to the trusted keystore}"
  : "${TACO_KEYSTORE_PASSWORD:?set TACO_KEYSTORE_PASSWORD}"

  output="$(jarsigner -verify -verbose -certs -strict \
    -keystore "${TACO_KEYSTORE}" \
    -storepass:env TACO_KEYSTORE_PASSWORD \
    "${taco}" 2>&1)" || status=$?

  if [ "${status}" -ne 0 ] || ! grep -q '^jar verified\.$' <<<"${output}"; then
    echo "${output}" >&2
    echo "Signature verification failed for ${taco} (jarsigner status ${status})" >&2
    return 1
  fi

  grep -E 'jar verified|X.509' <<<"${output}" | sed 's/^/  /'
}

case "${1:-}" in
  validate) validate ;;
  build) shift; build "$@" ;;
  sign) shift; sign "$@" ;;
  verify) shift; verify "$@" ;;
  *)
    echo "usage: $(basename "$0") {validate|build [dest-dir]|sign <taco>|verify <taco>}" >&2
    exit 2
    ;;
esac
