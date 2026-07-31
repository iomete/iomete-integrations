#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_VERSION="3.3.0"
PYTHON_VERSION="3.12"
RUN_ALL=false

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Run the iomete-airflow-plugin test suite inside Docker.

Options:
  --airflow-version VERSION   Airflow version to test against (default: 3.3.0)
  --python-version VERSION    Python version to use (default: 3.12)
  --all                       Run the full CI matrix: Airflow {3.2.2, 3.3.0} x Python {3.12, 3.13}
  -h, --help                  Show this help message
EOF
}

run_tests() {
    local af_ver="$1"
    local py_ver="$2"
    local tag="iomete-airflow-plugin-test:af${af_ver}-py${py_ver}"

    echo "========================================"
    echo "Testing: Airflow ${af_ver} / Python ${py_ver}"
    echo "========================================"

    docker build \
        --build-arg AIRFLOW_VERSION="${af_ver}" \
        --build-arg PYTHON_VERSION="${py_ver}" \
        -t "${tag}" \
        -f Dockerfile.test \
        .

    docker run --rm "${tag}"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --airflow-version)
            AIRFLOW_VERSION="$2"
            shift 2
            ;;
        --python-version)
            PYTHON_VERSION="$2"
            shift 2
            ;;
        --all)
            RUN_ALL=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

cd "$(dirname "$0")"

if [ "$RUN_ALL" = true ]; then
    FAILURES=0
    for af in 3.2.2 3.3.0; do
        for py in 3.12 3.13; do
            if ! run_tests "$af" "$py"; then
                FAILURES=$((FAILURES + 1))
            fi
        done
    done
    if [ "$FAILURES" -gt 0 ]; then
        echo "FAILED: ${FAILURES} combination(s) failed."
        exit 1
    fi
    echo "All matrix combinations passed."
else
    run_tests "$AIRFLOW_VERSION" "$PYTHON_VERSION"
fi
