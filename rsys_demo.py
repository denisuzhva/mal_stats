from dotenv import load_dotenv
import os
import logging
import configparser

from etl.csv_to_db import etl_main
from etl.db_manager import DatabaseManager


if __name__ == '__main__':
    load_dotenv()

    fmt = '[%(levelname)s] %(asctime)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=fmt)

    config = configparser.ConfigParser()
    config.read('cfg/baseconfig.ini')

    host = os.getenv('HOST')
    port = os.getenv('PORT')
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    db_manager = DatabaseManager(host, port, user, password, dbname)
    if config.getboolean('ETL', 'do_etl'):
        etl_main(db_manager, do_extract=False,
                 data_dir='O:/dev/_ml/mal_stats/data/svanoo')
