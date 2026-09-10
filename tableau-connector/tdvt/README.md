# Run TDVT locally

Tableau's Test Datasource Verification Tool (TDVT) exercises the connector through Tableau Desktop and a live IOMETE cluster. Run these commands from `tableau-connector/`.

You need:

- macOS with Tableau Desktop 2024.2
- the macOS region set to United States
- the IOMETE JDBC driver in `~/Library/Tableau/Drivers/`
- a running non-production IOMETE compute cluster
- a writable test catalog and schema

Tableau reads the tests' `#7/4/1972#` literals with the macOS region, so any other region turns July 4th into April 7th.

```bash
defaults write -g AppleLocale -string en_US
```

The generated virtual environment, TDS files, credentials, logs, and results stay in the ignored `tdvt/.local/` directory.

## 1. Set up TDVT

Set the object-store folder and non-production destination. If Tableau is not installed at the default Apple silicon path, also set its location:

```bash
export IOMETE_TESTV1_URI='s3a://your-bucket/tableau/TestV1'
export IOMETE_CATALOG='your-catalog'
export IOMETE_SCHEMA='TestV1'
export TABLEAU_APP='/Applications/Tableau Desktop (Apple silicon) 2024.2.app'
./tdvt/run.py setup
```

`IOMETE_TESTV1_URI` is the object-store folder where you will upload the two TestV1 CSV files. `IOMETE_CATALOG` and `IOMETE_SCHEMA` identify the non-production destination where the SQL creates the test tables. You can set `TABQUERYTOOL` instead of `TABLEAU_APP` when you need to point directly to the executable.

Setup validates the connector, downloads the pinned Tableau Connector SDK, installs TDVT in `tdvt/.local/venv`, and writes `tdvt/.local/load-testv1.sql`.

## 2. Load the TestV1 data

Upload these downloaded SDK files to the `IOMETE_TESTV1_URI` folder:

```text
~/.cache/iomete-tableau-connector/connector-plugin-sdk-tableau-2024.2/tests/datasets/TestV1/Calcs_headers.csv
~/.cache/iomete-tableau-connector/connector-plugin-sdk-tableau-2024.2/tests/datasets/TestV1/Staples_utf8_headers.csv
```

Run `tdvt/.local/load-testv1.sql` in IOMETE SQL Editor. It drops and recreates the `calcs` and `staples` tables in the selected schema.

## 3. Generate the TDS files with Tableau

Move any installed IOMETE `.taco` out of Tableau's connector directory:

```bash
mkdir -p "$HOME/Documents/My Tableau Repository/Connectors.disabled"
find "$HOME/Documents/My Tableau Repository/Connectors" -maxdepth 1 \
  -type f -iname '*iomete*.taco' \
  -exec mv {} "$HOME/Documents/My Tableau Repository/Connectors.disabled/" \;
```

Then start Tableau with the connector from this checkout:

```bash
TABLEAU_APP="${TABLEAU_APP:-/Applications/Tableau Desktop (Apple silicon) 2024.2.app}"
"$TABLEAU_APP/Contents/MacOS/Tableau" -DConnectPluginsPath="$PWD"
```

In Tableau, select **IOMETE (Arrow Flight SQL)**, enter the connection details, sign in, and open the TestV1 schema. Drag `calcs` onto the data source canvas, keep the connection **Live**, then select **Data → [data source] → Add to Saved Data Sources** and save it as `cast_calcs.iomete.tds`. Create another IOMETE data source and repeat the process for `staples`, saving it as `Staples.iomete.tds`.

Save the files at these exact paths:

```text
tdvt/.local/tds/cast_calcs.iomete.tds
tdvt/.local/tds/Staples.iomete.tds
```

Use the `calcs` table for `cast_calcs.iomete.tds` and the `staples` table for `Staples.iomete.tds`. Do not correct inferred types in the files; the runner validates them so connector metadata regressions fail before the suite starts.

Prepare the files with TDVT's standard datasource action:

```bash
(cd tdvt/.local && ./venv/bin/python -m tdvt.tdvt action --add_ds iomete)
```

Use these answers:

| Prompt | Answer |
|---|---|
| Set up a password file? | `n` |
| Schema other than TestV1? | `y`, then enter `IOMETE_SCHEMA`; use `n` when it is exactly `TestV1` |
| Different Staples or Calcs column names? | `n` |
| Run against a custom table? | `n` |
| Logical configuration | `iomete` |
| Overwrite the existing INI file? | `n`; the runner generates it |

Regenerate both TDS files and rerun this action before validating connector changes. These files are local test inputs and must not be committed.

## 4. Run the suite

Export a current access token and run TDVT:

```bash
export IOMETE_ACCESS_TOKEN='your-access-token'
./tdvt/run.py run
```

`TDVT_THREADS` sets how many test suites run at once and defaults to `6`. Each thread is another `tabquerytool` process with its own cluster connection. Use `TDVT_THREADS=1` for serial output when chasing a flaky failure.

The runner validates the prepared TDS files. It writes the token to `tdvt/.local/tds/iomete.password` with mode `0600` during the run and removes it afterward.

A successful run exits with status `0`. Review:

```text
tdvt/.local/test_results_combined.csv
tdvt/.local/tdvt_output_combined.json
```

Use Tableau's [TDVT failure guide](https://tableau.github.io/connector-plugin-sdk/docs/tdvt-test-case) to investigate failures.

After testing, restore the installed connector:

```bash
find "$HOME/Documents/My Tableau Repository/Connectors.disabled" -maxdepth 1 \
  -type f -iname '*iomete*.taco' \
  -exec mv {} "$HOME/Documents/My Tableau Repository/Connectors/" \;
```
