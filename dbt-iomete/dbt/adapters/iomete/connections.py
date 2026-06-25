from contextlib import contextmanager

from dbt.adapters.contracts.connection import Credentials, ConnectionState, AdapterResponse
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.exceptions import DbtProfileError
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.dataclass_schema import ValidationError
from dbt_common.utils.encoding import DECIMALS

from datetime import datetime

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import time

logger = AdapterLogger("Spark")

NUMBERS = DECIMALS + (int, float)
IOMETE_DEFAULT_CATALOG_NAME = "spark_catalog"


@dataclass
class SparkCredentials(Credentials):
    database: Optional[str] = None  # type: ignore
    schema: Optional[str] = None  # type: ignore
    https: bool = True
    host: Optional[str] = None
    port: int = 443
    dataplane: Optional[str] = None
    domain: Optional[str] = None
    lakehouse: Optional[str] = None
    user: Optional[str] = None
    token: Optional[str] = None
    connect_retries: int = 0
    connect_timeout: int = 120
    server_side_parameters: Dict[str, Any] = field(default_factory=dict)
    retry_all: bool = False

    _ALIASES = {
        'catalog': 'database',
    }

    def __post_init__(self):
        if self.database is not None and not self.database.strip():
            raise ValidationError(f"Invalid catalog name : {self.database}.")
        if self.database is None:
            self.database = IOMETE_DEFAULT_CATALOG_NAME

        if "." in (self.schema or ""):
            raise DbtRuntimeError(
                f"The schema should not contain '.': {self.schema}\n"
                "If you are trying to set a catalog, please use `catalog` instead.\n"
            )
        return

    @property
    def type(self):
        return 'iomete'

    @property
    def scheme(self):
        return 'https' if self.https else 'http'

    @property
    def unique_field(self):
        return f"{self.scheme}://{self.host}:{self.port}/dataplane/{self.dataplane}/lakehouse/{self.lakehouse}"

    def _connection_keys(self):
        return 'host', 'port', 'dataplane', 'lakehouse', 'database', 'schema'


class SparkConnectCursor(object):
    """A minimal DBAPI-style cursor on top of a Spark Connect ``SparkSession``.

    dbt's ``SQLConnectionManager`` drives SQL through a cursor that exposes
    ``execute``/``fetchall``/``description``. Spark Connect has no DBAPI layer,
    so we wrap ``spark.sql(...)`` here. SQL commands (DDL/DML) execute eagerly
    on the server when ``spark.sql`` is called; SELECTs are materialized lazily
    on ``fetchall``/``fetchone`` via ``collect()``.
    """

    def __init__(self, spark):
        self._spark = spark
        self._df = None
        self._rows: Optional[List] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def description(self) -> List[Tuple]:
        if self._df is None:
            return []
        return [
            (f.name, f.dataType.simpleString(), None, None, None, None, None)
            for f in self._df.schema.fields
        ]

    def execute(self, sql, bindings=None):
        sql = sql.strip().rstrip(";\n")

        if bindings is not None:
            bindings = [self._fix_binding(binding) for binding in bindings]
            sql = sql % tuple(bindings)

        # Reset any rows materialized from a previous statement.
        self._rows = None
        self._df = self._spark.sql(sql)

    def fetchall(self):
        if self._df is None:
            return None
        if self._rows is None:
            self._rows = [tuple(row) for row in self._df.collect()]
        return self._rows

    def fetchone(self):
        rows = self.fetchall()
        if not rows:
            return None
        return rows[0]

    def cancel(self):
        try:
            self._spark.interruptAll()
        except Exception as exc:
            logger.debug("Exception while cancelling query: {}".format(exc))

    def close(self):
        self._df = None
        self._rows = None

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to SQL literals that can be interpolated
        into the statement before it is sent to Spark Connect."""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return "'" + value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + "'"
        elif isinstance(value, str):
            return "'" + value.replace("'", "\\'") + "'"
        elif value is None:
            return "NULL"
        else:
            return "'" + str(value) + "'"


class SparkConnectConnectionWrapper(object):
    """Wrap a Spark Connect session so it looks like a DBAPI connection and
    no-ops transactions (Spark has none)."""

    def __init__(self, spark):
        self.spark = spark
        self._cursor: Optional[SparkConnectCursor] = None

    def cursor(self):
        self._cursor = SparkConnectCursor(self.spark)
        return self._cursor

    def cancel(self):
        if self._cursor:
            self._cursor.cancel()

    def close(self):
        if self._cursor:
            self._cursor.close()
        # NOTE: Do NOT call self.spark.stop() here. SparkSession.builder.remote(...)
        # .getOrCreate() returns a session backed by a shared gRPC channel, so every
        # dbt thread holds the same channel. Stopping it on a per-connection close
        # tears the channel down for all other in-flight threads ("Channel closed!"
        # / CANCELLED). The Spark Connect client is released automatically at process
        # exit, so we only drop our cursor reference here.

    def rollback(self, *args, **kwargs):
        pass


class SparkConnectionManager(SQLConnectionManager):
    TYPE = 'iomete'

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            raise DbtRuntimeError(str(exc)) from exc

    def cancel(self, connection):
        connection.handle.cancel()

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        # https://github.com/dbt-labs/dbt-spark/issues/142
        message = 'OK'
        return AdapterResponse(
            _message=message
        )

    # No transactions on Spark....
    def add_begin_query(self, *args, **kwargs):
        pass

    def add_commit_query(self, *args, **kwargs):
        pass

    def commit(self, *args, **kwargs):
        pass

    def rollback(self, *args, **kwargs):
        pass

    @classmethod
    def validate_creds(cls, creds, required):
        for key in required:
            if not hasattr(creds, key):
                raise DbtProfileError(f"The config '{key}' is required to connect to iomete")

            if creds.__dict__[key] is None:
                raise DbtProfileError(
                    f"The config '{key}' is set to none! This config is required to connect to iomete")

    @classmethod
    def _build_remote_url(cls, creds) -> str:
        """Build the Spark Connect remote URL (``sc://host:port/;param=value``).

        Reserved params understood by the Spark Connect client are ``use_ssl``
        and ``token`` (a bearer token, which requires ``use_ssl=true``). Any
        other param is forwarded to the server as a gRPC metadata header, which
        is how IOMETE routes the request to the right lakehouse/dataplane.
        """
        params: List[str] = [f"use_ssl={'true' if creds.https else 'false'}"]
        if creds.token:
            params.append(f"api_token={creds.token}")
        # Routing headers consumed by the IOMETE gateway.
        if creds.lakehouse:
            params.append(f"cluster={creds.lakehouse}")
        if creds.dataplane:
            params.append(f"data_plane={creds.dataplane}")

        return f"sc://{creds.host}:{creds.port}/;" + ";".join(params)

    @classmethod
    def _connect(cls, creds):
        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            raise FailedToConnectError(
                "pyspark with Spark Connect support is required for the iomete adapter. "
                "Install it with `pip install \"pyspark[connect]\"`."
            ) from e

        builder = SparkSession.builder.remote(cls._build_remote_url(creds))
        for key, value in (creds.server_side_parameters or {}).items():
            builder = builder.config(key, value)

        spark = builder.getOrCreate()
        return SparkConnectConnectionWrapper(spark)

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug('Connection is already open, skipping open.')
            return connection

        creds = connection.credentials
        exc = None

        for i in range(1 + creds.connect_retries):
            try:
                cls.validate_creds(creds, ['host', 'port', 'user', 'token', 'lakehouse', 'dataplane'])
                handle = cls._connect(creds)
                break
            except Exception as e:
                exc = e
                retryable_message = _is_retryable_error(e)
                if retryable_message and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {retryable_message}\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                elif creds.retry_all and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {getattr(exc, 'message', 'No message')}, "
                        f"retrying due to 'retry_all' configuration "
                        f"set to true.\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                else:
                    raise FailedToConnectError(
                        'Failed to connect! Make sure host, port, protocol (https/http) is correct '
                        'and that the lakehouse exposes a Spark Connect endpoint!'
                    ) from e
        else:
            raise exc

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection


def _is_retryable_error(exc: Exception) -> Optional[str]:
    message = str(getattr(exc, 'message', exc) or "").lower()
    if not message:
        return None
    if 'pending' in message:
        return message
    if 'temporarily_unavailable' in message:
        return message
    return None