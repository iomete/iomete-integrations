import configparser
import os
import re
import shlex
import subprocess
import venv
import xml.etree.ElementTree as ET
from pathlib import Path

SDK_TAG = os.environ.get("CONNECTOR_SDK_TAG", "tableau-2024.2")
CACHE_DIR = Path(
    os.environ.get(
        "TACO_CACHE_DIR", str(Path.home() / ".cache/iomete-tableau-connector")
    )
)
SDK_DIR = CACHE_DIR / f"connector-plugin-sdk-{SDK_TAG}"
TDVT_ROOT = Path(__file__).resolve().parent
CONNECTOR_ROOT = TDVT_ROOT.parent
LOCAL_DIR = TDVT_ROOT / ".local"
PYTHON = LOCAL_DIR / "venv/bin/python"
DEFAULT_TABLEAU_APP = Path("/Applications/Tableau Desktop (Apple silicon) 2024.2.app")
TDS_TYPES = {
    "cast_calcs.iomete.tds": {
        "bool0": "boolean",
        "bool1": "boolean",
        "bool2": "boolean",
        "bool3": "boolean",
        "time1": "string",
    },
    "Staples.iomete.tds": {
        "Order Quantity": "real",
        "Supplier Balance": "real",
        "Order Date": "datetime",
        "Product Base Margin": "real",
        "Received Date": "datetime",
        "Ship Date": "datetime",
    },
}


class TdvtError(Exception):
    pass


def execute(*args, cwd=None):
    command = [str(arg) for arg in args]
    print(f"+ {shlex.join(command)}")
    subprocess.run(command, cwd=cwd, check=True)


def required_value(name):
    result = os.environ.get(name)
    if not result:
        raise TdvtError(f"Set {name}.")
    if any(character in result for character in "\n\r\0"):
        raise TdvtError(f"{name} contains an unsupported control character.")
    return result


def validate_schema(schema):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise TdvtError(
            "The TestV1 schema must contain only letters, numbers, and underscores."
        )
    return schema


def tableau_cli():
    if os.environ.get("TABQUERYTOOL"):
        return Path(required_value("TABQUERYTOOL")).expanduser()

    if os.environ.get("TABLEAU_APP"):
        return (
            Path(required_value("TABLEAU_APP")).expanduser()
            / "Contents/MacOS/tabquerytool"
        )

    override = LOCAL_DIR / "config/tdvt/tdvt_override.ini"

    if override.is_file():
        config = configparser.ConfigParser()
        config.read(override)
        configured = config.defaults().get("tab_cli_exe_mac")
        if configured:
            return Path(configured).expanduser()

    return DEFAULT_TABLEAU_APP / "Contents/MacOS/tabquerytool"


def write_tableau_override(path):
    override = LOCAL_DIR / "config/tdvt/tdvt_override.ini"
    override.parent.mkdir(parents=True, exist_ok=True)
    override.write_text(
        f"[DEFAULT]\nTAB_CLI_EXE_MAC = {path}\nTAB_CLI_EXE_MAC_ARM = {path}\n",
        encoding="utf-8",
    )


def replace_placeholders(document, replacements, label):
    for placeholder, replacement in replacements.items():
        document = document.replace(placeholder, str(replacement))

    unresolved = sorted(set(re.findall(r"__[A-Z0-9_]+__", document)))
    if unresolved:
        raise TdvtError(f"Unresolved placeholders in {label}: {', '.join(unresolved)}")
    return document


def render_sql():
    uri = required_value("IOMETE_TESTV1_URI").rstrip("/")

    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9+.-]*://[^'\r\n]+", uri):
        raise TdvtError("IOMETE_TESTV1_URI must be an object-store URI without quotes.")

    catalog = required_value("IOMETE_CATALOG").replace("`", "``")
    schema = validate_schema(required_value("IOMETE_SCHEMA"))
    source = TDVT_ROOT / "sql/load-testv1.sql"
    output = LOCAL_DIR / "load-testv1.sql"
    document = replace_placeholders(
        source.read_text(encoding="utf-8"),
        {
            "__IOMETE_CATALOG__": catalog,
            "__IOMETE_SCHEMA__": schema,
            "__IOMETE_TESTV1_URI__": uri,
        },
        source.name,
    )
    output.write_text(document, encoding="utf-8")
    print(f"Spark SQL: {output}")


def setup():
    execute(CONNECTOR_ROOT / "scripts/taco.sh", "validate")
    if not (SDK_DIR / "tdvt").is_dir():
        raise TdvtError(f"TDVT not found at {SDK_DIR / 'tdvt'}.")

    tabquerytool = tableau_cli()

    if not os.access(tabquerytool, os.X_OK):
        raise TdvtError(
            f"tabquerytool not found at {tabquerytool}. Set TABLEAU_APP or TABQUERYTOOL."
        )

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    if not os.access(PYTHON, os.X_OK):
        venv.EnvBuilder(with_pip=True).create(LOCAL_DIR / "venv")

    # The pinned TDVT imports pkg_resources, which setuptools 81 removes.
    installed = subprocess.run(
        [PYTHON, "-c", "import tdvt, pkg_resources"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if installed.returncode:
        execute(
            PYTHON,
            "-m",
            "pip",
            "install",
            "--quiet",
            "setuptools<81",
            "-e",
            SDK_DIR / "tdvt",
        )

    if not (LOCAL_DIR / "config/tdvt").is_dir():
        execute(PYTHON, "-m", "tdvt.tdvt", "action", "--setup", cwd=LOCAL_DIR)

    write_tableau_override(tabquerytool)

    print(f"TDVT is ready at {LOCAL_DIR}.")

    if os.environ.get("IOMETE_TESTV1_URI"):
        render_sql()
    else:
        print(
            "Set IOMETE_TESTV1_URI, IOMETE_CATALOG, and IOMETE_SCHEMA, then rerun "
            "setup to generate the Spark SQL."
        )


def read_tds(path):
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError) as error:
        raise TdvtError(f"Cannot read {path}: {error}") from error

    connections = [
        element
        for element in root.iter("connection")
        if element.get("class") == "iomete"
    ]
    if len(connections) != 1 or not connections[0].get("schema"):
        raise TdvtError(
            f"{path.name} must contain one IOMETE connection with a schema."
        )

    named_connections = list(root.iter("named-connection"))
    relations = [
        element
        for element in root.iter()
        if isinstance(element.tag, str)
        and element.tag.endswith("relation")
        and element.get("connection") is not None
    ]
    if (
        len(named_connections) != 1
        or named_connections[0].get("name") != "leaf"
        or connections[0].get("tdvtconnection") != "iomete_connection"
        or not relations
        or any(relation.get("connection") != "leaf" for relation in relations)
    ):
        raise TdvtError(
            f"Prepare {path.name} with TDVT action --add_ds iomete."
        )

    metadata = {}
    for record in root.iter("metadata-record"):
        fields = {child.tag: child.text for child in record}
        if fields.get("remote-name") and fields.get("local-type"):
            metadata[fields["remote-name"]] = fields["local-type"]
    return validate_schema(connections[0].get("schema")), metadata


def validate_tds_files():
    schemas = set()
    errors = []

    for filename, expected_columns in TDS_TYPES.items():
        path = LOCAL_DIR / "tds" / filename
        if not path.is_file():
            errors.append(f"{filename} is missing")
            continue

        try:
            schema, actual_columns = read_tds(path)
        except TdvtError as error:
            errors.append(str(error))
            continue

        schemas.add(schema)
        for column, expected_type in expected_columns.items():
            if actual_columns.get(column) != expected_type:
                errors.append(
                    f"{filename}: {column} is "
                    f"{actual_columns.get(column, 'missing')}, expected {expected_type}"
                )

    if len(schemas) > 1:
        errors.append("the TDS files use different schemas")
    if errors:
        raise TdvtError(
            "Regenerate both Live TDS files with Tableau:\n  " + "\n  ".join(errors)
        )
    return schemas.pop()


def render_config(schema):
    source = TDVT_ROOT / "config/iomete.ini"
    output = LOCAL_DIR / "config/iomete.ini"
    document = replace_placeholders(
        source.read_text(encoding="utf-8"),
        {
            "__IOMETE_SCHEMA__": schema,
            "__CONNECTOR_ROOT__": CONNECTOR_ROOT,
        },
        source.name,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(document, encoding="utf-8")


def require_us_region():
    """Tableau parses the tests' #M/D/YYYY# literals with the macOS region."""
    result = subprocess.run(
        ["defaults", "read", "-g", "AppleLocale"],
        capture_output=True,
        text=True,
    )
    locale = result.stdout.strip()
    if not locale.endswith("_US"):
        raise TdvtError(
            f"macOS region is {locale or 'unset'}; TDVT date tests need United States.\n"
            "  defaults write -g AppleLocale -string en_US\n"
            "Quit Tableau afterwards so the new region is picked up."
        )


def run_tdvt():
    if not os.access(PYTHON, os.X_OK):
        raise TdvtError("Run ./tdvt/run.py setup first.")

    require_us_region()

    tabquerytool = tableau_cli()

    if not os.access(tabquerytool, os.X_OK):
        raise TdvtError(
            f"tabquerytool not found at {tabquerytool}. Run setup after setting "
            "TABLEAU_APP or TABQUERYTOOL."
        )

    write_tableau_override(tabquerytool)

    token = required_value("IOMETE_ACCESS_TOKEN")
    schema = validate_tds_files()
    render_config(schema)

    password_file = LOCAL_DIR / "tds/iomete.password"
    try:
        descriptor = os.open(password_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise TdvtError(
            f"Remove the existing {password_file} before running TDVT."
        ) from error

    try:
        with os.fdopen(descriptor, "w") as password:
            password.write(f"iomete_connection;{token}\n")
        execute(PYTHON, "-m", "tdvt.tdvt", "run", "iomete", "--generate", cwd=LOCAL_DIR)
    finally:
        password_file.unlink(missing_ok=True)

    print("Results:")
    print(f"  {LOCAL_DIR / 'test_results_combined.csv'}")
    print(f"  {LOCAL_DIR / 'tdvt_output_combined.json'}")
