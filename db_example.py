from pprint import pprint
from src.db_manager import DatabaseManager


if __name__ == "__main__":
    from dotenv import load_dotenv
    import os
    import argparse

    load_dotenv()

    def command_args():
        parser = argparse.ArgumentParser()
        parser.add_argument('-D', '--dir', required=True,
                            help='Path to query files')
        parser.add_argument('-q', '--query', required=True,
                            help='Query file')
        args = parser.parse_args()
        return args

    host = os.getenv('HOST')
    port = os.getenv('PORT')
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    db_manager = DatabaseManager(host, port, user, password, dbname)

    args = command_args()
    with open(args.dir + args.query, 'r') as query_f:
        query = query_f.read()
    data = db_manager.fetch_result(query)
    pprint(data)
