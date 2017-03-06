''' Routines to update gargantis tables on every interval. '''
import time
import datetime

from sqlalchemy import literal

from postgres import InsertFromSelect, model


def create_pg_stat_user_tables(session):
    session.execute('''
        CREATE SCHEMA IF NOT EXISTS gargantis;

        -- pg_stat_user_tables
        CREATE TABLE gargantis.pg_stat_user_tables (like pg_catalog.pg_stat_user_tables INCLUDING ALL);

        ALTER TABLE gargantis.pg_stat_user_tables
            ADD COLUMN date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc');

        CREATE INDEX ix_pg_stat_user_tables_date ON gargantis.pg_stat_user_tables USING btree (date);

        ''')

def create_pg_stat_activity(session):
    session.execute('''
        CREATE SCHEMA IF NOT EXISTS gargantis;

        -- pg_stat_activity
        CREATE TABLE gargantis.pg_stat_activity (like pg_catalog.pg_stat_activity INCLUDING ALL);

        ALTER TABLE gargantis.pg_stat_activity
            ADD COLUMN date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc');

        CREATE INDEX ix_pg_stat_activity_date ON gargantis.pg_stat_activity USING btree (date);

        ''')


def create_pg_stat_ssl(session):
    session.execute('''
        CREATE SCHEMA IF NOT EXISTS gargantis;

        -- pg_stat_ssl
        CREATE TABLE gargantis.pg_stat_ssl (like pg_catalog.pg_stat_ssl INCLUDING ALL);

        ALTER TABLE gargantis.pg_stat_ssl
            ADD COLUMN date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc');

        CREATE INDEX ix_pg_stat_ssl_date ON gargantis.pg_stat_ssl USING btree (date);

        ''')


def create_pg_stat_replication(session):
    session.execute('''
        CREATE SCHEMA IF NOT EXISTS gargantis;

        -- pg_stat_replication
        CREATE TABLE gargantis.pg_stat_replication (like pg_catalog.pg_stat_replication INCLUDING ALL);

        ALTER TABLE gargantis.pg_stat_replication
            ADD COLUMN date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc');

        CREATE INDEX ix_pg_stat_replication_date ON gargantis.pg_stat_replication USING btree (date);

        ''')


def create_pg_buffercache(session):
    session.execute('''
        CREATE SCHEMA IF NOT EXISTS gargantis;
        CREATE EXTENSION IF NOT EXISTS pg_buffercache;

        -- pg_buffercache
        CREATE TABLE gargantis.pg_buffercache AS SELECT pg_buffercache.* FROM pg_buffercache LIMIT 0;

        ALTER TABLE gargantis.pg_buffercache
            ADD COLUMN date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc');

        CREATE INDEX ix_pg_buffercache_date ON gargantis.pg_buffercache USING btree (date);

        ''')


def create_all(session):
    create_pg_stat_user_tables(session)
    create_pg_stat_activity(session)
    create_pg_stat_ssl(session)
    create_pg_stat_replication(session)
    create_pg_buffercache(session)


def generic_catalog(session, pg_catalog_table):
    '''Logs each row of @pg_catalog_table into @gargantis_table at utc time.'''
    pg_catalog = model(session, 'pg_catalog', pg_catalog_table)
    gargantis = model(session, 'gargantis', pg_catalog_table)
    session.execute(InsertFromSelect(gargantis.__table__, session.query(pg_catalog).subquery()))


def pg_stat_activity(session):
    ''' Logs all pg_stat_ativity. '''
    generic_catalog(session, 'pg_stat_activity')


def pg_stat_user_tables(session):
    generic_catalog(session, 'pg_stat_user_tables')


def pg_stat_ssl(session):
    generic_catalog(session, 'pg_stat_ssl')


def pg_stat_replication(session):
    generic_catalog(session, 'pg_stat_replication')


def pg_buffercache(session):
    session.execute('''
        INSERT INTO gargantis.pg_buffercache
        SELECT pg_buffercache.*, NOW() AT TIME ZONE 'utc'
        FROM pg_buffercache; ''')


def insert_all(session):
    pg_stat_activity(session)
    pg_stat_user_tables(session)
    pg_stat_ssl(session)
    pg_stat_replication(session)
    pg_buffercache(session)




