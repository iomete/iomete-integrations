from contextlib import contextmanager

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    ConnectionState,
    Credentials,
)
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.exceptions import DbtProfileError
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.dataclass_schema import ValidationError
from dbt_common.utils.encoding import DECIMALS

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from datetime import datetime

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

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
    data_plane: Optional[str] = None
    domain: Optional[str] = None
    lakehouse: Optional[str] = None
    cluster: Optional[str] = None
    user: Optional[str] = None
    token: Optional[str] = None
    max_msg_size: int = 134217728
    connect_retries: int = 0
    connect_timeout: int = 120
    server_side_parameters: Dict[str, Any] = field(default_factory=dict)
    retry_all: bool = False

    _ALIASES = {
        'catalog': 'database',
        'data_plane': 'dataplane',
    }

    def __post_init__(self):
        if self.database is not None and not self.database.strip():
            raise ValidationError(f"Invalid catalog name : {self.database}.")
        if self.database is None:
            self.database = IOMETE_DEFAULT_CATALOG_NAME

        if "." in (self.schema or ""):
            raise DbtRuntimeError(
                f"The schema should not contain '.': {self.schema}\n"
                "If you are trying to set a catalog, please use "
                "`catalog` instead.\n"
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
        return (
            f"{self.scheme}://{self.host}:{self.port}"
            f"/dataplane/{self.data_plane_name}"
            f"/cluster/{self.cluster_name}"
        )

    def _connection_keys(self):
        return (
            'host',
            'port',
            'dataplane',
            'cluster',
            'lakehouse',
            'database',
            'schema',
        )

    @property
    def cluster_name(self):
        return self.cluster or self.lakehouse

    @property
    def data_plane_name(self):
        return self.dataplane or self.data_plane


class IometeSqlAlchemyConnectionWrapper(object):
    """Wrap a Spark connection in a way that no-ops transactions"""

    # https://forums.databricks.com/questions/2157/in-apache-spark-sql-can-we-roll-back-the-transacti.html  # noqa

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor:
            try:
                self._cursor.cancel()
            except EnvironmentError as exc:
                logger.debug(
                    "Exception while cancelling query: {}".format(exc)
                )

    def close(self):
        if self._cursor:
            try:
                self._cursor.close()
            except EnvironmentError as exc:
                logger.debug(
                    "Exception while closing cursor: {}".format(exc)
                )
        self.handle.close()

    def rollback(self, *args, **kwargs):
        pass

    def fetchall(self):
        return self._cursor.fetchall()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        is_write = self._is_write_statement(sql)
        if bindings is not None:
            bindings = tuple(
                self._fix_binding(binding) for binding in bindings
            )
            if is_write and hasattr(self._cursor, 'executemany'):
                self._cursor.executemany(sql, [bindings])
            else:
                self._cursor.execute(sql, bindings)
        else:
            self._cursor.execute(sql)

        if is_write:
            try:
                self._cursor.fetchall()
            except Exception as exc:
                logger.debug(
                    "Ignoring error while draining cursor: {}".format(exc)
                )

    @classmethod
    def _is_write_statement(cls, sql):
        tokens = sql.lstrip().split(None, 1)
        if not tokens:
            return False
        first_token = tokens[0].lower()
        return first_token in {
            'alter',
            'cache',
            'create',
            'delete',
            'drop',
            'insert',
            'merge',
            'refresh',
            'truncate',
            'uncache',
            'update',
            'use',
        }

    @classmethod
    def _fix_binding(cls, value):
        """Convert complex datatypes to primitives that can be loaded by
           the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        else:
            return value

    @property
    def description(self):
        return self._cursor.description


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

            response = exc.args[0]
            if hasattr(response, 'status'):
                msg = response.status.errorMessage
                raise DbtRuntimeError(msg)
            else:
                raise DbtRuntimeError(str(exc))

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
                raise DbtProfileError(
                    f"The config '{key}' is required to connect to iomete"
                )

            if creds.__dict__[key] is None:
                raise DbtProfileError(
                    f"The config '{key}' is set to none! This config is "
                    "required to connect to iomete"
                )

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug('Connection is already open, skipping open.')
            return connection

        creds = connection.credentials
        exc = None

        for i in range(1 + creds.connect_retries):
            try:
                cls.validate_creds(
                    creds,
                    ['host', 'port', 'user', 'token', 'schema'],
                )
                if creds.cluster_name is None:
                    raise DbtProfileError(
                        "One of the configs 'cluster' or 'lakehouse' is "
                        "required to connect to iomete"
                    )
                if creds.data_plane_name is None:
                    raise DbtProfileError(
                        "One of the configs 'data_plane' or 'dataplane' is "
                        "required to connect to iomete"
                    )

                conn = create_engine(
                    cls._connection_url(creds)
                ).raw_connection()
                handle = IometeSqlAlchemyConnectionWrapper(conn)
                break
            except Exception as e:
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a password is invalid, or something
                    msg = (
                        'Failed to connect. Make sure cluster is in '
                        'non-terminated state and credentials (user/password) '
                        'are correct'
                    )
                    raise FailedToConnectError(msg) from e
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
                        'Failed to connect! Make sure host, port, protocol '
                        '(https/http) is correct!'
                    ) from e
        else:
            raise exc

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def _connection_url(cls, creds):
        return URL.create(
            "iomete",
            username=creds.user,
            password=creds.token,
            host=creds.host,
            port=creds.port,
            database=f"{creds.database}/{creds.schema}",
            query={
                "cluster": creds.cluster_name,
                "data_plane": creds.data_plane_name,
                "tls": str(creds.https).lower(),
                "max_msg_size": str(creds.max_msg_size),
            },
        )


def _is_retryable_error(exc: Exception) -> Optional[str]:
    message = getattr(exc, 'message', None)
    if message is None:
        return None
    message = message.lower()
    if 'pending' in message:
        return exc.message
    if 'temporarily_unavailable' in message:
        return exc.message
    return None
