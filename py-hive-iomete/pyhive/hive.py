"""DB-API implementation backed by HiveServer2 (Thrift API)

See http://www.python.org/dev/peps/pep-0249/

Many docstrings in this file are based on the PEP, which is in the public domain.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

import base64
import datetime
import re
from decimal import Decimal
from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED, create_default_context


from TCLIService import TCLIService
from TCLIService import constants
from TCLIService import ttypes
from pyhive import common
from pyhive.common import DBAPITypeObject
# Make all exceptions visible in this module per DB-API
from pyhive.exc import *  # noqa
from builtins import range
import contextlib
from future.utils import iteritems
import getpass
import logging
import sys
import thrift.transport.THttpClient
import thrift.protocol.TBinaryProtocol
import thrift.transport.TSocket
import thrift.transport.TTransport

# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'

_logger = logging.getLogger(__name__)

_TIMESTAMP_PATTERN = re.compile(r'(\d+-\d+-\d+ \d+:\d+:\d+(\.\d{,6})?)')

ssl_cert_parameter_map = {
    "none": CERT_NONE,
    "optional": CERT_OPTIONAL,
    "required": CERT_REQUIRED,
}


def _parse_timestamp(value):
    if value:
        match = _TIMESTAMP_PATTERN.match(value)
        if match:
            if match.group(2):
                format = '%Y-%m-%d %H:%M:%S.%f'
                # use the pattern to truncate the value
                value = match.group()
            else:
                format = '%Y-%m-%d %H:%M:%S'
            value = datetime.datetime.strptime(value, format)
        else:
            raise Exception(
                'Cannot convert "{}" into a datetime'.format(value))
    else:
        value = None
    return value


TYPES_CONVERTER = {"DECIMAL_TYPE": Decimal,
                   "TIMESTAMP_TYPE": _parse_timestamp}


class HiveParamEscaper(common.ParamEscaper):
    def escape_string(self, item):
        # backslashes and single quotes need to be escaped
        # TODO verify against parser
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode('utf-8')
        return "'{}'".format(
            item
            .replace('\\', '\\\\')
            .replace("'", "\\'")
            .replace('\r', '\\r')
            .replace('\n', '\\n')
            .replace('\t', '\\t')
        )


_escaper = HiveParamEscaper()


def connect(*args, **kwargs):
    """Constructor for creating a connection to the database. See class :py:class:`Connection` for
    arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)


class Connection(object):
    """Wraps a Thrift session"""

    def __init__(
        self,
        host=None,
        port=443,
        scheme="https",

        lakehouse=None,
        data_plane=None,

        database='default',
        username=None,
        password=None,

        configuration=None,
        thrift_transport=None
    ):
        """Connect to HiveServer2

        :param host: What host HiveServer2 runs on
        :param port: What port HiveServer2 runs on. Defaults to 10000.
        :param configuration: A dictionary of Hive settings (functionally same as the `set` command)
        """
        if scheme in ("https", "http") and thrift_transport is None:
            ssl_context = None
            if scheme == "https":
                ssl_context = create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = CERT_NONE

            # Update URL logic
            if data_plane:
                url = "{scheme}://{host}:{port}/data-plane/{data_plane}/lakehouse/{lakehouse}".format(
                    scheme=scheme, host=host, port=port, data_plane=data_plane, lakehouse=lakehouse
                )
            else:
                url = "{scheme}://{host}:{port}/lakehouse/{lakehouse}".format(
                    scheme=scheme, host=host, port=port, lakehouse=lakehouse
                )

            thrift_transport = thrift.transport.THttpClient.THttpClient(
                uri_or_host=url,
                ssl_context=ssl_context,
            )

            self._set_authorization_header(
                thrift_transport, username, password)

        username = username or getpass.getuser()
        configuration = configuration or {}

        if thrift_transport is None:
            raise ValueError("thrift_transport cannot be none!")

        self._transport = thrift_transport

        protocol = thrift.protocol.TBinaryProtocol.TBinaryProtocol(
            self._transport)
        self._client = TCLIService.Client(protocol)
        # oldest version that still contains features we care about
        # "V6 uses binary type for binary payload (was string) and uses columnar result set"
        protocol_version = ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6

        try:
            self._transport.open()
            open_session_req = ttypes.TOpenSessionReq(
                client_protocol=protocol_version,
                configuration=configuration,
                username=username,
            )
            response = self._client.OpenSession(open_session_req)
            _check_status(response)
            assert response.sessionHandle is not None, "Expected a session from OpenSession"
            self._sessionHandle = response.sessionHandle
            assert response.serverProtocolVersion == protocol_version, \
                "Unable to handle protocol version {}".format(
                    response.serverProtocolVersion)
            with contextlib.closing(self.cursor()) as cursor:
                cursor.execute('USE {}'.format(database))
        except:
            self._transport.close()
            raise

    @staticmethod
    def _set_authorization_header(transport, username=None, password=None):
        username = username or "user"
        password = password or "pass"
        auth_credentials = "{username}:{password}".format(
            username=username, password=password
        ).encode("UTF-8")
        auth_credentials_base64 = base64.standard_b64encode(auth_credentials).decode(
            "UTF-8"
        )
        transport.setCustomHeaders(
            {
                "Authorization": "Basic {auth_credentials_base64}".format(
                    auth_credentials_base64=auth_credentials_base64
                )
            }
        )

    def __enter__(self):
        """Transport should already be opened by __init__"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Call close"""
        self.close()

    def close(self):
        """Close the underlying session and Thrift transport"""
        req = ttypes.TCloseSessionReq(sessionHandle=self._sessionHandle)
        response = self._client.CloseSession(req)
        self._transport.close()
        _check_status(response)

    def commit(self):
        """Hive does not support transactions, so this does nothing."""
        pass

    def cursor(self, *args, **kwargs):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(self, *args, **kwargs)

    @property
    def client(self):
        return self._client

    @property
    def sessionHandle(self):
        return self._sessionHandle

    def rollback(self):
        raise NotSupportedError(
            "Hive does not have transactions")  # pragma: no cover


class Cursor(common.DBAPICursor):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """

    def __init__(self, connection, arraysize=1000):
        self._operationHandle = None
        super(Cursor, self).__init__()
        self._arraysize = arraysize
        self._connection = connection

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        super(Cursor, self)._reset_state()
        self._description = None
        if self._operationHandle is not None:
            request = ttypes.TCloseOperationReq(self._operationHandle)
            try:
                response = self._connection.client.CloseOperation(request)
                _check_status(response)
            finally:
                self._operationHandle = None

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        """Array size cannot be None, and should be an integer"""
        default_arraysize = 1000
        try:
            self._arraysize = int(value) or default_arraysize
        except TypeError:
            self._arraysize = default_arraysize

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        This attribute will be ``None`` for operations that do not return rows or if the cursor has
        not had an operation invoked via the :py:meth:`execute` method yet.

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        if self._operationHandle is None or not self._operationHandle.hasResultSet:
            return None
        if self._description is None:
            req = ttypes.TGetResultSetMetadataReq(self._operationHandle)
            response = self._connection.client.GetResultSetMetadata(req)
            _check_status(response)
            columns = response.schema.columns
            self._description = []
            for col in columns:
                primary_type_entry = col.typeDesc.types[0]
                if primary_type_entry.primitiveEntry is None:
                    # All fancy stuff maps to string
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[ttypes.TTypeId.STRING_TYPE]
                else:
                    type_id = primary_type_entry.primitiveEntry.type
                    type_code = ttypes.TTypeId._VALUES_TO_NAMES[type_id]
                self._description.append((
                    col.columnName.decode(
                        'utf-8') if sys.version_info[0] == 2 else col.columnName,
                    type_code.decode(
                        'utf-8') if sys.version_info[0] == 2 else type_code,
                    None, None, None, None, True
                ))
        return self._description

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the operation handle"""
        self._reset_state()

    def execute(self, operation, parameters=None, **kwargs):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """
        # backward compatibility with Python < 3.7
        for kw in ['async', 'async_']:
            if kw in kwargs:
                async_ = kwargs[kw]
                break
        else:
            async_ = False

        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        self._reset_state()

        self._state = self._STATE_RUNNING
        _logger.info('%s', sql)

        req = ttypes.TExecuteStatementReq(self._connection.sessionHandle,
                                          sql, runAsync=async_)
        _logger.debug(req)
        response = self._connection.client.ExecuteStatement(req)
        _check_status(response)
        self._operationHandle = response.operationHandle

    def cancel(self):
        req = ttypes.TCancelOperationReq(
            operationHandle=self._operationHandle,
        )
        response = self._connection.client.CancelOperation(req)
        _check_status(response)

    def _fetch_more(self):
        """Send another TFetchResultsReq and update state"""
        assert(self._state ==
               self._STATE_RUNNING), "Should be running when in _fetch_more"
        assert(
            self._operationHandle is not None), "Should have an op handle in _fetch_more"
        if not self._operationHandle.hasResultSet:
            raise ProgrammingError("No result set")
        req = ttypes.TFetchResultsReq(
            operationHandle=self._operationHandle,
            orientation=ttypes.TFetchOrientation.FETCH_NEXT,
            maxRows=self.arraysize,
        )
        response = self._connection.client.FetchResults(req)
        _check_status(response)
        schema = self.description
        assert not response.results.rows, 'expected data in columnar format'
        if response.results.columns is None:
            columns = []
        else:
            columns = [_unwrap_column(col, col_schema[1]) for col, col_schema in
                   zip(response.results.columns, schema)]
        new_data = list(zip(*columns))
        self._data += new_data
        # response.hasMoreRows seems to always be False, so we instead check the number of rows
        # https://github.com/apache/hive/blob/release-1.2.1/service/src/java/org/apache/hive/service/cli/thrift/ThriftCLIService.java#L678
        # if not response.hasMoreRows:
        if not new_data:
            self._state = self._STATE_FINISHED

    def poll(self, get_progress_update=True):
        """Poll for and return the raw status data provided by the Hive Thrift REST API.
        :returns: ``ttypes.TGetOperationStatusResp``
        :raises: ``ProgrammingError`` when no query has been started
        .. note::
            This is not a part of DB-API.
        """
        if self._state == self._STATE_NONE:
            raise ProgrammingError("No query yet")

        req = ttypes.TGetOperationStatusReq(
            operationHandle=self._operationHandle,
            getProgressUpdate=get_progress_update,
        )
        response = self._connection.client.GetOperationStatus(req)
        _check_status(response)

        return response

    def fetch_logs(self):
        """Retrieve the logs produced by the execution of the query.
        Can be called multiple times to fetch the logs produced after the previous call.
        :returns: list<str>
        :raises: ``ProgrammingError`` when no query has been started
        .. note::
            This is not a part of DB-API.
        """
        if self._state == self._STATE_NONE:
            raise ProgrammingError("No query yet")

        try:  # Older Hive instances require logs to be retrieved using GetLog
            req = ttypes.TGetLogReq(operationHandle=self._operationHandle)
            logs = self._connection.client.GetLog(req).log.splitlines()
        except ttypes.TApplicationException as e:  # Otherwise, retrieve logs using newer method
            if e.type != ttypes.TApplicationException.UNKNOWN_METHOD:
                raise
            logs = []
            while True:
                req = ttypes.TFetchResultsReq(
                    operationHandle=self._operationHandle,
                    orientation=ttypes.TFetchOrientation.FETCH_NEXT,
                    maxRows=self.arraysize,
                    fetchType=1  # 0: results, 1: logs
                )
                response = self._connection.client.FetchResults(req)
                _check_status(response)
                assert not response.results.rows, 'expected data in columnar format'
                assert len(
                    response.results.columns) == 1, response.results.columns
                new_logs = _unwrap_column(response.results.columns[0])
                logs += new_logs

                if not new_logs:
                    break

        return logs


#
# Type Objects and Constructors
#


for type_id in constants.PRIMITIVE_TYPES:
    name = ttypes.TTypeId._VALUES_TO_NAMES[type_id]
    setattr(sys.modules[__name__], name, DBAPITypeObject([name]))


#
# Private utilities
#


def _unwrap_column(col, type_=None):
    """Return a list of raw values from a TColumn instance."""
    for attr, wrapper in iteritems(col.__dict__):
        if wrapper is not None:
            result = wrapper.values
            nulls = wrapper.nulls  # bit set describing what's null
            assert isinstance(nulls, bytes)
            for i, char in enumerate(nulls):
                byte = ord(char) if sys.version_info[0] == 2 else char
                for b in range(8):
                    if byte & (1 << b):
                        result[i * 8 + b] = None
            converter = TYPES_CONVERTER.get(type_, None)
            if converter and type_:
                result = [converter(row) if row else row for row in result]
            return result
    raise DataError("Got empty column value {}".format(col)
                    )  # pragma: no cover


def _check_status(response):
    """Raise an OperationalError if the status is not success"""
    _logger.debug(response)
    if response.status.statusCode != ttypes.TStatusCode.SUCCESS_STATUS:
        raise OperationalError(response)
