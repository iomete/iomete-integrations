<p align="center">
  <img
    src="https://iomete.com/resources/logo-black.svg"
    alt="iomete logo"
    width="300"
    align="middle"
  />
  &nbsp;&nbsp;&nbsp;
  <img
    src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg"
    alt="dbt logo"
    width="180"
    align="middle"
  />
</p>
<p align="center">
  <a href="https://github.com/iomete/iomete-integrations/actions/workflows/dbt-iomete-release.yml">
    <img src="https://github.com/iomete/iomete-integrations/actions/workflows/dbt-iomete-release.yml/badge.svg" alt="Release Badge"/>
  </a>
</p>

## dbt-iomete

The `dbt-iomete` package contains all the code enabling dbt to work with iomete.

This adapter is forked from the [dbt-spark](https://github.com/dbt-labs/dbt-spark)

## Getting started

### Installation

```shell
pip install dbt-iomete
```

Alternatively, you can install the package from GitHub with:

```shell
pip install "git+https://github.com/iomete/iomete-integrations.git#subdirectory=dbt-iomete"
```

### Profile Setup

```yaml
iomete:
  target: dev
  outputs:
    dev:
      type: iomete
      host: <host>
      port: 443
      https: true # or http
      dataplane: <iomete_dataplane>
      domain: <iomete_domain>
      lakehouse: <serverless_lakehouse_name>
      catalog: <catalog_name>
      schema: <database_name>
      user: "{{ env_var('DBT_IOMETE_USER_NAME') }}"
      token: "{{ env_var('DBT_IOMETE_TOKEN') }}"
      # optional: parallelism for listing relations in a schema (default 100)
      list_relations_threads: 100
```

`list_relations_threads` controls how many relations dbt describes in parallel when it
lists a schema (via `describe extended`). It is independent of the global `threads`
setting used to build models, so you can keep `threads` low while still listing schemas
with many tables quickly. It defaults to `100`; lower it if the data plane is under load,
or omit it entirely to use the default.

### Incremental strategies

IOMETE incremental models use Iceberg tables. Choose a strategy with the
`incremental_strategy` config:

| Strategy | Behavior |
| --- | --- |
| `merge` (default) | Updates rows that match the `unique_key` and inserts new rows. |
| `append` | Inserts every row from the current run without changing existing rows. |
| `delete+insert` | Deletes rows that match the current run's `unique_key` values, then inserts every row from the current run. |
| `insert_overwrite` | Replaces the affected partitions, or the whole table when `partition_by` is not set. |

#### Use `delete+insert` for duplicate keys

Use `delete+insert` when one run can return several rows with the same `unique_key`.
Spark rejects those rows during a `merge`, but `delete+insert` keeps them. If you omit
`unique_key`, this strategy behaves like `append`.

The delete and insert are separate statements, so the operation is not atomic. If the insert
fails after the delete succeeds, rerun the model or perform a full refresh to restore the
missing rows. Also note that `incremental_predicates` apply only to the delete. The insert
still writes every row from the current run.

#### Replace partitions with `insert_overwrite`

Use `insert_overwrite` when each run returns the complete contents of every partition it
updates. Rows already stored in an affected partition are removed, even if the current model
result does not contain replacements for them.

For example, this model replaces only the calendar days returned by the current run:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by='days(event_time)'
) }}

select event_id, event_time, payload
from {{ ref('events') }}
{% if is_incremental() %}
where event_time >= current_date() - interval 1 day
{% endif %}
```

Partitions absent from the result stay unchanged. Iceberg hidden partition transforms such
as `days(event_time)` and `bucket(16, event_id)` are supported. If you omit `partition_by`,
the current result replaces every row in the target table.

The overwrite is one atomic Iceberg operation. Values map to target columns by name rather
than source position, and a target column missing from the current model result receives
`NULL`.

For more information, consult [the docs](https://iomete.com/docs/guides/dbt/getting-started-with-iomete-dbt).
