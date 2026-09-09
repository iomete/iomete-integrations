# IOMETE Tableau Connector

The IOMETE Tableau Connector connects Tableau to IOMETE through the Arrow Flight SQL JDBC driver. The connector provides Tableau's connection dialog and SQL behavior; the JDBC driver is installed separately.

## Supported setup

| Component | Supported value |
|---|---|
| Connector version | `0.1.0` |
| Tableau | Desktop or Server 2024.2+ |
| Platforms | Windows, macOS, and Linux |
| Connection modes | Live and extract |
| Authentication | Username and Access token |
| Transport | JDBC with TLS |

## Install the connector

You need three files:

1. Open the [Tableau connector releases](https://github.com/iomete/iomete-integrations/releases?q=tableau-connector) page and choose the newest `tableau-connector-v*` release.
2. Download its `.taco` file and `iomete-taco.cer`. The release also includes `SHA256SUMS` for verifying both files.
3. Download the tested JDBC driver, [`flight-sql-jdbc-driver-19.0.0-iomete.3.jar`](https://github.com/iomete/iomete-artifacts/raw/refs/heads/main/flight-sql-jdbc-driver-19.0.0-iomete.3.jar).

The connector does not bundle the JDBC driver.

Place the connector and JDBC driver in their standard Tableau directories:

| Platform | `.taco` connector | JDBC driver |
|---|---|---|
| macOS Desktop | `~/Documents/My Tableau Repository/Connectors/` | `~/Library/Tableau/Drivers/` |
| Windows Desktop | `C:\Users\[user]\Documents\My Tableau Repository\Connectors` | `C:\Program Files\Tableau\Drivers` |
| Linux Server | `/opt/tableau/connectors` | `/opt/tableau/tableau_driver/jdbc` |
| Windows Server | `C:\Program Files\Tableau\Connectors` | `C:\Program Files\Tableau\Drivers` |

On Tableau Server, install both files in the same locations on every node, make them readable by the Tableau Server run-as account, and restart Tableau Server. Restart Tableau Desktop after installing or replacing either file.


### Trust the connector certificate

For now, IOMETE signs the `.taco` with a self-signed certificate. Tableau cannot trust this certificate automatically, so you need to import `iomete-taco.cer` once. Future releases will use a certificate from a publicly trusted certificate authority, which will remove this manual step.

The same certificate signs every self-signed release. Import it again only if IOMETE rotates the certificate or a Tableau upgrade replaces the JRE truststore.

On macOS Desktop, set `TABLEAU_JRE` to the JRE inside your installed application, then back up its truststore and import the certificate. This example matches Tableau Desktop 2026.2 for Apple silicon; adjust the application name and version if yours differs:

```bash
TABLEAU_JRE="/Applications/Tableau Desktop (Apple silicon) 2026.2.app/Contents/Plugins/jre"
sudo cp "$TABLEAU_JRE/lib/security/cacerts" "$TABLEAU_JRE/lib/security/cacerts.bak"
sudo keytool -importcert -noprompt -alias iomete-taco \
  -file /path/to/iomete-taco.cer \
  -keystore "$TABLEAU_JRE/lib/security/cacerts" -storepass changeit
```


On Windows Desktop, run PowerShell as Administrator and adjust the Tableau version in the path:

```powershell
$TableauJre = 'C:\Program Files\Tableau\Tableau <version>\Plugins\jre'
Copy-Item "$TableauJre\lib\security\cacerts" "$TableauJre\lib\security\cacerts.bak"
keytool -importcert -noprompt -alias iomete-taco `
  -file C:\path\to\iomete-taco.cer `
  -keystore "$TableauJre\lib\security\cacerts" -storepass changeit
```

Restart Tableau Desktop after the import. Tableau Server does not document a stable JRE truststore path; confirm the path for your exact installation and repeat the import on every node, or use the testing-only verification bypass below.

### Disable connector signature verification for testing

If you cannot edit the truststore, start Tableau Desktop from the command line with signature verification disabled. On macOS, adjust the application name and version first:

```bash
TABLEAU_APP="/Applications/Tableau Desktop (Apple silicon) 2026.2.app"
"$TABLEAU_APP/Contents/MacOS/Tableau" \
  -DDisableVerifyConnectorPluginSignature=true
```

On Windows, run:

```powershell
& 'C:\Program Files\Tableau\Tableau <version>\bin\tableau.exe' `
  '-DDisableVerifyConnectorPluginSignature=true'
```

On Tableau Server, disable verification through TSM and apply the pending change:

```bash
tsm configuration set -k native_api.disable_verify_connector_plugin_signature -v true --force-keys
tsm pending-changes apply
```

Disabling connector signature verification allows Tableau to load any unsigned or untrusted connector, so use it only for testing.

## Connect to IOMETE

In Tableau, open **Connect → To a Server → More…**, then choose **IOMETE (Arrow Flight SQL)**.

Fill in the connection dialog:

| Field | What to enter |
|---|---|
| Server | Your IOMETE host, without `https://` |
| Port | `443`, unless your deployment uses another port |
| Compute Cluster | The IOMETE compute cluster name |
| Namespace | The IOMETE namespace name |
| Catalog | Optional. Leave blank for all catalogs, or enter one catalog to limit discovery |
| Username | Your IOMETE username |
| Access Token | Your IOMETE access token |
| Query Timeout | Optional. Maximum query duration in seconds; blank means no timeout |
| Connection Timeout | Optional. Connection timeout in seconds; blank uses the driver's 10-second default |
| Client Thread Pool Size | Optional. Parallel Flight endpoint workers; blank uses the driver's default of `1` |
| Disable Certificate Verification | Leave off unless the IOMETE endpoint uses a certificate Tableau does not trust; disabling it weakens server identity verification |

Make sure the compute cluster is running, then select **Sign In**. Leave **Catalog** blank when you want to browse every catalog you can access. If you enter a catalog, Tableau limits metadata discovery to that catalog.

TLS encryption is always enabled. **Disable Certificate Verification** skips validation of the IOMETE server's TLS certificate; it does not disable encryption or bypass `.taco` signature verification. The connector passes the username and access token as JDBC properties, so credentials are not included in the JDBC URL.

## Troubleshooting

- If the connector does not appear, confirm the connector and driver are in the correct directories, then either import `iomete-taco.cer` into Tableau's JRE truststore or disable signature verification for testing.
- If Tableau cannot find the driver, confirm the filename is `flight-sql-jdbc-driver-19.0.0-iomete.3.jar` and that it is in the driver directory for the Tableau host.
- If a valid connection fails, confirm that the compute cluster is running and that the server, cluster, namespace, username, and access token are correct.

## Maintain the connector

Run commands from the `tableau-connector` directory.

### Test and validate

The contract tests use Node's built-in test runner and do not require Tableau or a live IOMETE endpoint. Validate the XML with the pinned Tableau Connector SDK after the tests pass:

```bash
node --test tests/*.test.js
./scripts/taco.sh validate
```

Pull requests that change `tableau-connector/**` run both checks in `.github/workflows/tableau-connector-pr-check.yml`.

### Build locally

Build an unsigned development package in `build/`, or pass another destination to the command:

```bash
./scripts/taco.sh build /tmp/taco-out
```

Load local builds by disabling signature verification as described above. The release workflow handles signing and verification.

### Release

1. Update `plugin-version` in `connector/manifest.xml` and merge the change to `main`.
2. Run the **RELEASE for tableau-connector** workflow manually.
3. Confirm the GitHub Release contains the `.taco`, `iomete-taco.cer`, and `SHA256SUMS` files.

The workflow runs the PR checks, builds and self-signs the connector, verifies it against the protected `tableau-release` keystore, and publishes the release.

Normal releases reuse the same certificate. If it approaches expiry or the key is compromised, replace `TACO_KEYSTORE_BASE64` and `TACO_KEYSTORE_PASSWORD` in the `tableau-release` environment. Customers must then import the new `iomete-taco.cer`.
