"""Integration between SQLAlchemy and Trino.

Some code based on
https://github.com/zzzeek/sqlalchemy/blob/rel_0_5/lib/sqlalchemy/databases/sqlite.py
which is released under the MIT license.
"""

from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import types

from pyhive import hive
from pyhive.sqlalchemy_hive import HiveCompiler, HiveTypeCompiler, HiveHTTPDialect, HiveHTTPSDialect, HiveIdentifierPreparer, HiveDate, \
    HiveTimestamp, HiveDecimal, HiveDialect


class IometeIdentifierPreparer(HiveIdentifierPreparer):
    pass


_type_map = {
    'boolean': types.Boolean,
    'tinyint': types.SmallInteger,
    'smallint': types.SmallInteger,
    'int': types.Integer,
    'bigint': types.BigInteger,
    'float': types.Float,
    'double': types.Float,
    'string': types.String,
    'varchar': types.String,
    'char': types.String,
    'date': HiveDate,
    'timestamp': HiveTimestamp,
    'binary': types.String,
    'array': types.String,
    'map': types.String,
    'struct': types.String,
    'uniontype': types.String,
    'decimal': HiveDecimal,
}


class IometeCompiler(HiveCompiler):
    pass


class IometeTypeCompiler(HiveTypeCompiler):
    pass


class IometeHttpDialect(HiveDialect):
    name = 'iomete'
    scheme = "http"
    driver = "rest"

    supports_statement_cache = False

    def create_connect_args(self, url):
        kwargs = {
            "host": url.host,
            "port": url.port or 80,
            "scheme": self.scheme,
            "username": url.username or None,
            "password": url.password or None,
            "database": url.database or None
        }
        if url.query:
            kwargs.update(url.query)
            return [], kwargs
        return ([], kwargs)

    @classmethod
    def dbapi(cls):
        return hive

class IometeHttpsDialect(HiveDialect):
    name = 'iomete'
    scheme = "https"
    driver = "rest"

    supports_statement_cache = False

    def create_connect_args(self, url):
        kwargs = {
            "host": url.host,
            "port": url.port or 443,
            "scheme": self.scheme,
            "username": url.username or None,
            "password": url.password or None,
            "database": url.database or None
        }
        if url.query:
            kwargs.update(url.query)
            return [], kwargs
        return ([], kwargs)

    @classmethod
    def dbapi(cls):
        return hive
