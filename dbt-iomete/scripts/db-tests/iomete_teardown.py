#!/usr/bin/env python
"""Remove the IOMETE resources ``iomete_provision.py`` created for a test run.

Reads the provision state file and deletes everything recorded there, in reverse
order of creation, tolerating resources that are already gone. It deliberately
does **not** touch catalogs: ``test_dbt_multi_catalog`` is shared test
infrastructure (created only if missing, never removed) and ``spark_catalog`` is
the built-in default.

The admin token comes from the same ``DBT_IOMETE_*`` environment as provisioning
(it is never read from the state file). Run this after the suites, including on
failure — the bash runner wires it into an ``EXIT`` trap.

Usage::

    python scripts/iomete_teardown.py                 # default state file
    python scripts/iomete_teardown.py --state-file PATH
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Optional

from iomete_provision import (
    DEFAULT_STATE_FILE,
    Config,
    IometeClient,
    ProvisionError,
    _load_dotenv,
)

logger = logging.getLogger("iomete_teardown")


def teardown(config: Config, state_path: str) -> None:
    if not os.path.isfile(state_path):
        logger.info("No state file at %s; nothing to tear down.", state_path)
        return

    with open(state_path) as handle:
        state = json.load(handle)
    created = state.get("created", {})
    client = IometeClient(config)

    username = created.get("username")
    role_name = created.get("role_name")
    membership_id = created.get("membership_id")
    ns_bundle_id = created.get("ns_bundle_id")
    compute_id = created.get("compute_id")
    policy_id = created.get("policy_id")

    # Reverse creation order; each call tolerates an already-deleted resource.
    if compute_id:
        logger.info("Deleting compute %s", compute_id)
        client.delete_compute(compute_id)
    if policy_id is not None:
        logger.info("Deleting access policy %s", policy_id)
        client.delete_access_policy(policy_id)
    if username and ns_bundle_id:
        logger.info("Revoking compute permissions for %s", username)
        client.revoke_bundle_actor(username, ns_bundle_id)
    if username and role_name:
        logger.info("Revoking create role %s", role_name)
        client.revoke_create_role(username, role_name)
    if membership_id:
        logger.info("Removing domain membership %s", membership_id)
        client.remove_domain_member(membership_id)
    if username:
        logger.info("Deleting test user %s", username)
        client.delete_user(username)

    os.remove(state_path)
    logger.info("Teardown complete; removed state file %s", state_path)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [teardown] %(levelname)s %(message)s"
    )

    try:
        _load_dotenv()
        config = Config.from_env()
        teardown(config, args.state_file)
    except ProvisionError as exc:
        logger.error("Teardown failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
