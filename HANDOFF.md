# Handoff — Integration test runtime improvements

## Goal
Reduce runtime of dbt-iomete **integration + functional** tests. Baseline: ~15 min
even with `-n20` parallelism. Branch is the start of a series of runtime levers, not
a single fix.

## Where things live
- **Worktree (work here):** `/Users/abhishekpathania/Documents/work/iomete-integrations/dbt-iomete-metadata-poll`
  - This is the **monorepo** root; the adapter + tests are under `dbt-iomete/`.
- **Branch:** `perf/integration-test-runtime` (pushed to `origin`, tracking set).
- **venv:** `<worktree>/.venv` — already has the adapter (`pip install -e .`) + `dev-requirements.txt`.
- **Cluster creds:** `<original-repo>/.env` at
  `/Users/abhishekpathania/Documents/work/iomete-integrations/dbt-iomete/.env`
  (gitignored, 8 `DBT_IOMETE_*` keys). Lakehouse `dbt` on `release.iomete.cloud:443`.

### Run tests
```bash
cd <worktree>/dbt-iomete
. ../.venv/bin/activate
set -a; . /Users/abhishekpathania/Documents/work/iomete-integrations/dbt-iomete/.env; set +a
pytest -q tests/integration/<suite> -p no:cacheprovider --no-header -rA
```

## Done (committed + pushed, verified against live lakehouse)
Both touch only `dbt-iomete/tests/integration/base.py`. See commits for diffs/rationale:
- `a5603e3` — replace blind `time.sleep(30)` in `assertTablesEqual` with
  `_wait_for_fresh_metadata` (poll describe until visible + same arity; returns the
  fetched columns so `_assertTableColumnsEqual` reuses them, no double `describe`).
  **Measured −29s/call** (A/B probe: NEW ~15s vs OLD ~44s, always settles in 1 attempt).
- `eafd3ec` — collapse teardown to a single `DROP SCHEMA ... CASCADE`; removed the
  redundant per-object drop loop + `_get_schemas/_get_views_in_schema/_get_tables_in_schema/...`.
  Verified CASCADE fully removes managed Iceberg tables+views. **~26s+/class teardown.**

PR is **not** opened (user asked push-only). Create link:
https://github.com/iomete/iomete-integrations/pull/new/perf/integration-test-runtime

## Key findings (don't re-derive these)
- `schema_service` is **NOT** in the `assertTablesEqual` path. Column metadata comes
  from `describe extended` over Thrift (`impl.py:128`); `schema_service` only backs
  relation *listing* (`impl.py:97`).
- Metadata is **not stale** post-write — poll settles on the 1st attempt every time.
  The old 30s sleep guarded nothing real.
- Dominant per-test costs (from `TestDefaultAppend` logs, ~327s for 1 assertion):
  - ~13–15s connect/warmup **per `dbt` invocation** (`seed_and_run_twice` = 3 invocations).
  - `describe extended` ~7–15s each.
  - `-n20` contention on one shared lakehouse.
- **Cluster wall-clock noise is ±~4 min** on identical work — single end-to-end runs
  can't prove a ~2-min saving. Use isolated A/B probes (see below) for clean numbers.
- Lakehouse **auto-suspends**; a raw `pyhive.connect` then throws `EOFError`. Wrap any
  direct probe connect in a retry loop (the dbt adapter survives via `connect_retries=5`).

## Remaining levers (not started — user deferred)
1. **dbt-invocation connect overhead** (biggest, riskier): reuse one connection/adapter
   across seed+run+run instead of ~14s reconnect per invocation. Touches `run_dbt` path.
2. **`-n` vs compute tuning:** measure whether `-n20` oversaturates the lakehouse; find
   worker count minimizing wall-clock. Infra/config, in `tox.ini` (`-n20` at the
   integration + functional envs).
3. **Merge functional+integration** into one `pytest -n20` run (CI runs them as separate
   sequential steps → idle workers + double xdist startup).
4. **`connect_retries: 5` + `connect_timeout: 60`** (`conftest.py` / `get_profile`) → a
   single flaky connect can burn 300s silently; lower for tests to fail fast.

## Throwaway probe scripts
Lived in this session's scratchpad (session-specific, may be gone):
`probe_timing.py` (A/B sleep-vs-poll) and `probe_cascade.py` (CASCADE safety). Both just
open a `pyhive` connection from `.env`, create/drop a temp schema, and print timings.
Re-create from the descriptions above if needed; remember the connect-retry wrapper.

## Suggested skills for the next session
- `pr-description-writer` (Task agent) — when opening the PR (per user rules: Summary/
  Context/Implementation only, **no** Test plan section, **no** Claude attribution).
- `verify` / `run` — to validate a change against the live app/cluster.
- `code-review` — review the diff before PR.
- `diagnose` — if chasing the connect-overhead lever / flaky connects.
