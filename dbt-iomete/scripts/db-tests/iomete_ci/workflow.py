"""Manage the temporary IOMETE environment used by dbt CI tests.

Each test run gets an isolated user, compute, role, and access policy. This
module creates those resources, checks that the new user can run a basic query,
and removes everything after the run.

Created resources are written to the state file immediately after each step. If
the process exits early, the state file still contains enough information for
``teardown`` to clean up anything that was already created.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import time

from .client import IometeClient
from .config import Config
from .errors import ProvisionError
from .state import ProvisionState

logger = logging.getLogger(__name__)


def provision(config: Config, state_path: str) -> ProvisionState:
    client = IometeClient(config)
    state = ProvisionState(state_path, config.domain)

    suffix = secrets.token_hex(4)
    username = f"dbtci-user-{suffix}"
    compute_name = f"dbtci-compute-{suffix}"
    role_name = f"dbtci-role-{suffix}"
    policy_name = f"dbtci-policy-{suffix}"

    password = client.create_user(username)
    logger.info("Created test user %r", username)
    state.set_created(username=username)

    user_token = client.login_user(username, password)
    membership_id = client.add_domain_member(username)
    state.set_created(membership_id=membership_id)

    client.create_catalogs_if_missing()

    # Grant required permissions
    client.grant_create_role(username, role_name)
    state.set_created(role_name=role_name)
    ns_bundle_id = client.resolve_namespace_bundle()
    client.grant_bundle_perms(username, ns_bundle_id)
    state.set_created(ns_bundle_id=ns_bundle_id)

    # Wait for syncing permissions
    time.sleep(10)

    # Create PAT for temp user for creating cluster, data policies &
    # running queries on the cluster
    pat = client.create_access_token(user_token, f"dbtci-pat-{suffix}")

    state.set_test_env(
        DBT_IOMETE_TOKEN=pat,
        DBT_IOMETE_USER_NAME=username,
    )

    compute_id = client.create_compute(compute_name, user_token, ns_bundle_id)
    state.set_created(compute_id=compute_id, compute_name=compute_name)
    state.set_test_env(DBT_IOMETE_LAKEHOUSE=compute_name)

    client.wait_compute_active(compute_id, user_token)

    policy_id = client.create_full_access_policy(policy_name, username, config.catalogs)
    state.set_created(policy_id=policy_id)

    logger.info("Provisioning complete. State written to %s", state_path)

    return state


def _read_state(state_path: str) -> dict:
    if not os.path.isfile(state_path):
        raise ProvisionError(
            f"State file {state_path} not found. Run `provision` before `preflight`."
        )
    with open(state_path) as handle:
        return json.load(handle)


def preflight(config: Config, state_path: str) -> None:
    """Open a real connection as the test user and run ``SELECT 1``.

    This is the closest check to what the suites do: it proves the test user can
    reach the compute and query through the same thrift path the dbt adapter uses.
    """
    state = _read_state(state_path)
    env = state.get("test_env", {})
    username = env.get("DBT_IOMETE_USER_NAME")
    token = env.get("DBT_IOMETE_TOKEN")
    compute = env.get("DBT_IOMETE_LAKEHOUSE")

    if not (username and token and compute):
        raise ProvisionError(f"State file {state_path} is missing connection details.")

    try:
        from pyhive import hive
    except ImportError:
        raise ProvisionError(
            "pyhive not importable; falling back to a control-plane reachability check."
        )

    logger.info(
        "Preflight: connecting to compute %r as %r and running SELECT 1",
        compute,
        username,
    )

    def attempt() -> None:
        conn = hive.connect(
            scheme=config.scheme,
            host=config.host,
            port=config.port,
            lakehouse=compute,
            database=config.catalogs[0],
            username=username,
            password=token,
            data_plane=config.namespace,
        )
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchall()
            if not result or result[0][0] != 1:
                raise ProvisionError(
                    f"Preflight SELECT 1 returned unexpected result: {result!r}"
                )
        finally:
            conn.close()

    # driverStatus can report ACTIVE a little before the thrift gateway accepts
    # sessions, so give the warmup a few tries before declaring failure.
    deadline = time.time() + config.active_timeout_seconds

    while True:
        try:
            attempt()
            logger.info("Preflight OK: the test user can query the compute.")
            return
        except Exception as exc:
            if time.time() >= deadline:
                raise ProvisionError(
                    f"Preflight failed after warmup retries: {exc}"
                ) from exc
            logger.info(
                "Preflight not ready yet (%s); retrying ...", type(exc).__name__
            )
            time.sleep(config.poll_interval_seconds)


def teardown(config: Config, state_path: str) -> None:
    """Delete everything a provision run recorded, in reverse creation order.

    Tolerates resources that are already gone. Deliberately does **not** touch
    catalogs: ``test_dbt_multi_catalog`` is shared test infrastructure (created
    only if missing, never removed) and ``spark_catalog`` is the built-in default.
    """
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
        client.revoke_bundle_perms(username, ns_bundle_id)
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
