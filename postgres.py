''' Some sqlalchemy helpers. '''
import functools
import os
import socket
from contextlib import contextmanager

from setproctitle import getproctitle
from sqlalchemy import create_engine, Table
from sqlalchemy.ext import compiler
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.query import Query
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.expression import ClauseElement, Executable
from sqlalchemy.schema import MetaData


__all__ = ('session', 'model', 'InsertFromSelect')


class ManagedSession(Session):

    def __init__(self, *args, **kwargs):
        kwargs['query_cls'] = Query
        super(ManagedSession, self).__init__(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, typ, value, traceback):
        try:
            self.close()
        except AttributeError as ex:
            if '_Connection__connection' in ex.message:
                return
            raise


def memoize(func):
    cache = {}

    def get_method_instance(func, args):
        self = getattr(func, 'im_self', None)
        if self:
            return self
        self = args[0] if args else None
        self_class = getattr(self, '__class__', None)
        func_name = getattr(func, 'func_name', None)
        return self if func_name and hasattr(self_class, func_name) else None

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        def get_or_set(key, cache):
            if key not in cache:
                cache[key] = func(*args, **kwargs)
            return cache[key]
        key = (args, frozenset(kwargs.items()))
        self = get_method_instance(func, args)
        if self:
            self._memoize_cache = getattr(self, '_memoize_cache', {})
            key = (func, args[1:], frozenset(kwargs.items()))
            return get_or_set(key, self._memoize_cache)
        return get_or_set(key, cache)
    return wrapper


@memoize
def model(session, schema, tablename):
    ''' Returns sqlalchemy model. '''
    metadata = MetaData(session.bind, schema=schema, reflect=True)
    table = Table(tablename, metadata, autoload=True, autoload_with=session.bind)
    # hack to reflect tables without primary keys by making primary key the row.
    primary_key = table.primary_key if table.primary_key else table.columns
    Base = declarative_base()
    class Model(Base):
        ''' there must be a better way. '''
        __table__ = table
        __mapper_args__ = {"primary_key": primary_key}
    return Model


class InsertFromSelect(Executable, ClauseElement):
    def __init__(self, table, select):
        self.table = table
        self.select = select


@compiler.compiles(InsertFromSelect)
def visit_insert_from_select(element, compiler, **kw):
    return "INSERT INTO %s (%s)" % (
        compiler.process(element.table, asfrom=True),
        compiler.process(element.select))


@contextmanager
def session(uri, sync=False, autoflush=False, expire_on_commit=False):
    ''' Returns a managed session to the postgres server. '''
    application_name = ('%s:%s:%05d' % (getproctitle(), socket.gethostname(), os.getpid()))[-63:]
    connect_args = {'application_name': application_name}
    engine = create_engine(uri, connect_args=connect_args, poolclass=StaticPool)
    with ManagedSession(engine, autoflush=autoflush, expire_on_commit=expire_on_commit) as sesh:
        sesh.execute('SET synchronous_commit TO OFF;') if not sync else None
        yield sesh
