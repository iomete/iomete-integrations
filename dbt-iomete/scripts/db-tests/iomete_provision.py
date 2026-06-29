#!/usr/bin/env python
"""Provision the isolated IOMETE resources the dbt-iomete test suites need.

The integration and functional suites run as a short-lived *test user* against a
freshly created compute, with full query access to the catalogs the tests use.
This script creates that environment with an admin token and records everything
it creates to a state file so ``iomete_teardown.py`` can remove it afterwards.

The flow (all against the existing ``DBT_IOMETE_DOMAIN`` domain):

1. create a temp user and log it in (its token is what the suites connect with);
2. add the user to the domain;
3. ensure the required catalogs exist (create-if-missing, never deleted);
4. grant the user a domain role that allows creating compute, plus operate
   permissions on the data-plane namespace bundle;
5. create the compute *as the user*, start it, and wait until it is ACTIVE;
6. create a data-security access policy granting the user full access to the
   catalogs;
7. preflight ``SELECT 1`` as the user.

State is written incrementally, so a crash mid-provision still leaves a state
file teardown can act on. The admin token is never written to disk.

The implementation lives in the ``iomete_ci`` package alongside this file; this
script is just the command-line entrypoint.

Usage::

    python scripts/db-tests/iomete_provision.py provision    # create resources + write state
    python scripts/db-tests/iomete_provision.py preflight    # SELECT 1 as the test user
    python scripts/db-tests/iomete_provision.py all          # provision then preflight (default)

    # --state-file PATH  (default: dbt-iomete/scripts/db-tests/.provision-state.json)
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

from iomete_ci import (
    DEFAULT_STATE_FILE,
    Config,
    ProvisionError,
    load_dotenv,
    preflight,
    provision,
)

logger = logging.getLogger("iomete_provision")


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])

    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=["provision", "preflight", "all"],
        help="provision: create resources; preflight: SELECT 1 as the test user; all: both (default)",
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
        if args.command in ("preflight", "all"):
            preflight(config, args.state_file)
    except ProvisionError as exc:
        logger.error("Provisioning failed: %s", exc)

        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
