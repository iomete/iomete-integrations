#!/usr/bin/env python
"""Remove the IOMETE resources ``iomete_provision.py`` created for a test run.

Reads the provision state file and deletes everything recorded there, in reverse
order of creation, tolerating resources that are already gone. Also removes the
``dbt-iomete/.env.test`` credentials file the provision step wrote.

The admin token (``DBT_IOMETE_ADMIN_TOKEN``) comes from the same ``DBT_IOMETE_*``
environment as provisioning. Run this after the suites, 
including on failure — the bash runner wires it into an ``EXIT`` trap.

The implementation lives in the ``iomete_ci`` package alongside this file; this
script is just the command-line entrypoint.

Usage::

    python scripts/db-tests/iomete_teardown.py                 # default state file
    python scripts/db-tests/iomete_teardown.py --state-file PATH
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
    teardown,
)

logger = logging.getLogger("iomete_teardown")


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [teardown] %(levelname)s %(message)s"
    )

    try:
        load_dotenv()

        config = Config.from_env()

        teardown(config, args.state_file)
    except ProvisionError as exc:
        logger.error("Teardown failed: %s", exc)

        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
