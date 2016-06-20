''' Routines to update gargantis tables on every interval. '''
import time
import datetime

from sqlalchemy import literal

from postgres import InsertFromSelect, model


def create_gargantis_tables(session, contrib=False):
    session.execute('''
        CREATE SCHEMA gargantis;

        -- pg_stat_activity
        CREATE TABLE gargantis.pg_stat_activity (
            like pg_catalog.pg_stat_activity INCLUDING ALL);

        ALTER TABLE gargantis.pg_stat_activity
            ADD COLUMN date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc');

        CREATE INDEX ix_pg_stat_activity_date ON gargantis.pg_stat_activity
            USING btree (date);

        -- pg_stat_user_tables
        CREATE TABLE gargantis.pg_stat_user_tables (
            like pg_catalog.pg_stat_user_tables INCLUDING ALL);

        ALTER TABLE gargantis.pg_stat_user_tables
            ADD COLUMN date TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc');

        CREATE INDEX ix_pg_stat_user_tables_date ON gargantis.pg_stat_user_tables
            USING btree (date);
        ''')


def generic_catalog(session, pg_catalog_table):
    '''Logs each row of @pg_catalog_table into @gargantis_table at utc time.'''
    PgCalalog = model(session, 'pg_catalog', pg_catalog_table)
    Gargantis = model(session, 'gargantis', pg_catalog_table)
    subquery = (
        session.
        query(PgCalalog, literal(datetime.datetime.utcnow()).label('date')).
        subquery())
    session.execute(InsertFromSelect(Gargantis.__table__, subquery))


def pg_stat_activity(session):
    ''' Logs all pg_stat_ativity. '''
    generic_catalog(session, 'pg_stat_activity')


def pg_stat_user_tables(session):
    generic_catalog(session, 'pg_stat_user_tables')






