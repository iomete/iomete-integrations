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

For more information, consult [the docs](https://iomete.com/docs/guides/dbt/getting-started-with-iomete-dbt).
