import argparse
import time
import __main__

import postgres
import gargantis


def collect_stats(session, interval):
    while True:
        time.sleep(interval if interval >= 1 else 10)
        gargantis.insert_all(session)
        session.commit()


def main():
    parser = argparse.ArgumentParser(description=__main__.__doc__)
    parser.add_argument('--uri', type=str, help='Database connection string.')
    parser.add_argument('--interval', type=int, help='Data will be logged every @interval secs.')
    parser.add_argument('--init', action='store_true', help='Init the gargantis tables.')
    parsed = parser.parse_args()
    with postgres.session(parsed.uri) as session:
        if parsed.init:
            gargantis.create_all(session)
            session.commit()
        collect_stats(session, interval if parsed.interval else None)

if __name__ == '__main__':
    main()
