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

Incremental models run on Iceberg tables and support three strategies, set with the
`incremental_strategy` config:

| Strategy | What it does |
| --- | --- |
| `merge` (default) | Updates target rows matching the unique key and inserts the rest. |
| `append` | Inserts every source row, never touching existing rows. |
| `delete+insert` | Deletes the target rows whose unique key appears in the new data, then inserts every source row. |

Reach for `delete+insert` when the source can produce more than one row per unique key.
Spark's `MERGE INTO` fails in that case, while `delete+insert` keeps all of them. Without a
`unique_key` it behaves like `append`.

Two caveats worth knowing before you use it:

- The delete and the insert are two separate statements, and Spark cannot wrap them in one
  transaction. If the insert fails after the delete has succeeded, the deleted rows are gone
  and the model has to be rerun (or full-refreshed) to get them back.
- `incremental_predicates` apply to the delete only, never to the insert. A predicate that
  excludes an existing target row from the delete leaves that row in place while the insert
  still adds the new one, so you end up with two rows sharing a key.

For more information, consult [the docs](https://iomete.com/docs/guides/dbt/getting-started-with-iomete-dbt).
