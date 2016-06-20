import argparse
import time
import __main__

import postgres
import gargantis


def collect_stats(session, interval):
    while True:
        time.sleep(interval if interval >= 1 else 5)
        gargantis.pg_stat_activity(session)
        gargantis.pg_stat_user_tables(session)
        session.commit()


def main():
    parser = argparse.ArgumentParser(description=__main__.__doc__)
    parser.add_argument('--uri', type=str, help='Database connection string.')
    parser.add_argument('--interval', type=int, help='Data will be logged every @interval secs.')
    parsed = parser.parse_args()
    with postgres.session(parsed.uri) as session:
        collect_stats(session, interval if parsed.interval else None)

if __name__ == '__main__':
    main()
