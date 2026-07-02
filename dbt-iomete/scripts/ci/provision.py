#!/usr/bin/env python
"""Provision the isolated IOMETE resources the dbt-iomete test suites need.

The integration and functional suites run as a short-lived *test user* against a
freshly created compute, with full query access to the catalogs the tests use.
This script creates that environment with the admin token
(``DBT_IOMETE_ADMIN_TOKEN``) and records every resource it creates to a state
file so ``teardown.py`` can remove it afterwards.

The flow (all against the existing ``DBT_IOMETE_DOMAIN`` domain):

1. create a temp user and log it in (its token is what the suites connect with);
2. add the user to the domain;
3. create a uniquely-named catalog for the multi-catalog snapshot tests and
   attach it to the domain so the compute can see it (removed at teardown;
   ``spark_catalog`` is the built-in default);
4. grant the user a domain role that allows creating compute, plus operate
   permissions on the data-plane namespace bundle;
5. create the compute *as the user*, start it, and wait until it is ACTIVE;
6. create a data-security access policy granting the user full access to the
   catalogs;
7. write the test-user credentials to ``dbt-iomete/.env.test`` (loaded by
   pytest-dotenv) so the suites connect as the provisioned user;
8. healthcheck ``SELECT 1`` as the user.

State is written incrementally, so a crash mid-provision still leaves a state
file teardown can act on. The admin token is never written to disk.

The implementation lives in the ``resources`` package alongside this file; this
script is just the command-line entrypoint.

Usage::

    python scripts/ci/provision.py provision    # create resources + write state
    python scripts/ci/provision.py healthcheck  # start compute if stopped, then SELECT 1
    python scripts/ci/provision.py all          # provision then healthcheck (default)

    # --state-file PATH  (default: dbt-iomete/scripts/ci/.provision-state.json)
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from resources import (
    DEFAULT_STATE_FILE,
    Config,
    ProvisionError,
    load_dotenv,
    healthcheck,
    provision,
)

logger = logging.getLogger("provision")


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])

    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=["provision", "healthcheck", "all"],
        help="provision: create resources; healthcheck: start compute if stopped, then SELECT 1 as the test user; all: both (default)",
    )
    parser.add_argument(
        "--state-file",
        default=DEFAULT_STATE_FILE,
        help=f"where to read/write the provision state (default: {DEFAULT_STATE_FILE})",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [provision] %(levelname)s %(message)s"
    )

    try:
        load_dotenv()

        config = Config.from_env()

        if args.command in ("provision", "all"):
            provision(config, args.state_file)
        if args.command in ("healthcheck", "all"):
            healthcheck(config)
    except ProvisionError as exc:
        logger.error("Provisioning failed: %s", exc)

        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
