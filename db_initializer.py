from pprint import pprint
from etl.db_manager import DatabaseManager


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    import argparse

    load_dotenv()

    def command_args():
        parser = argparse.ArgumentParser()
        parser.add_argument('-q', '--query', required=False,
                            help='Path to query file')
        args = parser.parse_args()
        return args

    host = os.getenv('HOST')
    port = os.getenv('PORT')
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    tbs_name = 'mal'
    tbs_location = 'O:\dev\_ml\mal_stats\data\db'
    db_name = 'mal_stats'

    db_manager = DatabaseManager(host, port, user, password)
    db_manager.create_tablespace(tbs_name, tbs_location)
    db_manager.create_database(tbs_name, db_name)

    args = command_args()
    if args.query is not None:
        with open(args.query, 'r') as query_f:
            query = query_f.read()
        data = db_manager.fetch_result(query)
        pprint(data)
