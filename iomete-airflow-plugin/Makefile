dev_venv := venv
python := $(dev_venv)/bin/python
pip := $(dev_venv)/bin/pip
black := $(dev_venv)/bin/black
airflow_version := "1.1.3"

# Serve the dev environment for Airflow 1
serve_airflow1: _check_docker_compose
	docker-compose -p airflow1 -f docker-compose_dev_airflow1.yaml up --force-recreate --build --remove-orphans

# Serve the dev environment for Airflow 2
serve_airflow2: _check_docker_compose
	docker-compose -p airflow2 -f docker-compose_dev_airflow2.yaml up --force-recreate --build --remove-orphans

# Stop and remove all containers
clear_containers:
	docker-compose -f docker-compose_dev_airflow1.yaml rm --stop --force -v
	docker-compose -f docker-compose_dev_airflow2.yaml rm --stop --force -v

_check_docker_compose:
	#!/usr/bin/env bash
	if ! [ -x "$(command -v docker-compose)" ]; then
		echo 'Install a Docker engine, probably at https://docs.docker.com/install/'
		exit 1
	fi

# Remove all build, test, coverage and Python artifacts
clean: clean_build clean_pyc clean_test

# Remove build artifacts
clean_build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

# Remove Python file artifacts
clean_pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

# Remove test and coverage artifacts
clean_test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

# Package and upload a release
release: create_dev_venv dist
	{{python}} -m twine upload dist/*

# Build source and wheel package
dist: create_dev_venv clean
	{{python}} setup.py sdist
	{{python}} setup.py bdist_wheel
	ls -l dist

# Install the package to the active Python's site-packages
install: create_dev_venv clean
	{{python}} setup.py install

## Format python code base with Black
#format-black +opts='': create_dev_venv
#	{{black}} . {{opts}}

# Create a dev venv if not exist
create_dev_venv:
	#!/usr/bin/env bash
	if ! [ -d "./{{dev_venv}}" ]
	then
		echo "Creating a new development virtual env: {{dev_venv}} ..."
		python -m venv {{dev_venv}}
		echo "Installing development librairies ..."
		{{pip}} install -r ./requirements_dev.txt
	fi

# Delete dev venv
cleanup_dev_venv:
	rm -rf {{dev_venv}}
	rm -rf .mypy_cache

# Delete dev venv then recreate it
update_dev_venv: cleanup_dev_venv create_dev_venv

install:
	poetry install -vv

# Create virtualenv using poetry and install dependencies with dev-dependencies and optional dependencies
install-with-optional:
	poetry install -vv --with optional

# Update lock and create virtualenv using poetry and install dependencies with dev-dependencies
update-install:
	poetry update -vv

# Update lock and create virtualenv using poetry and install dependencies with dev-dependencies and optional dependencies
update-install-with-optional:
	poetry update -vv --with optional

# Create virtualenv using poetry and install dependencies without dev-dependencies
install-no-dev:
	poetry install --no-dev -vv

# Create virtualenv using poetry and install dependencies without dev-dependencies
update-install-no-dev:
	poetry update --no-dev -vv

# Update pip, wheel and setuptools. This step does not assume poetry
install-build-deps:
	python -m pip install -U pip wheel setuptools

# Run formatting on the package
format: isort blue

# Run formatting checks on the package
format-check: isort-check blue-check

# Run isort formatting on the package
isort:
	poetry run isort ./

# Run isort formatting check on the package
isort-check:
	poetry run isort --check ./

# Run blue formatting on the package
blue:
	poetry run blue ./

# Run blue formatting check on the package
blue-check:
	poetry run blue --check ./

# Run package test suit using pytest
test:
	poetry run pytest ./tests

# Run package test suit using pytest
test-no-poetry:
	python -m pytest ./tests

# Run package release acceptance tests - should be custom for the package
release-acceptance-tests: test

# Run mypy check on the package

# Run documentation coverage using interrogate
interrogate:
	poetry run interrogate .

# Build the package wheel and sdist
build-all:
	poetry build -vv

# Build the package wheel (the binaries)
build-wheel:
	poetry build -f wheel -vv

# Build the package sdist
build-sdist:
	poetry build -f sdist -vv

# Removes a generic wheel that matches the *-py3-*.whl, useful for publishing packages to prevent override
remove-generic-wheel:
	rm -f dist/*-py3-*.whl

# Package and publish the docs to causaLens' artifactory
package-and-publish-docs:
	poetry source add --priority=supplemental causalens https://us-central1-python.pkg.dev/causalens-internal/python-internal/simple
	poetry add --source=causalens docs-builder@~0.2.8
	poetry run python ./tooling/scripts/docs-upload.py

# Publish the package to PyPI
publish:
	poetry config pypi-token.pypi ${PYPI_TOKEN}
	poetry publish

# Check the package can import
self-import-check:
	python -c "import iomete-airflow-plugin"

# Check the package can import
self-import-check-poetry:
	poetry run python -c "import iomete-airflow-plugin"


