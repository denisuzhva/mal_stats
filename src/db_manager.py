from sqlalchemy import create_engine
from sqlalchemy import exc
from sqlalchemy import text

import pandas as pd


class DatabaseManager():
    def __init__(self, 
                 host: str, 
                 port: str, 
                 user: str, 
                 password: str, 
                 dbname: str,
                 dialect: str = "postgresql",
                 driver: str = "psycopg2"):
        self.engine = create_engine(
            "{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}".format(
                dialect=dialect,
                driver=driver,
                username=user,
                password=password,
                host=host,
                port=port,
                database=dbname
            )
        )

    def fetch_result(self, query: str) -> list:
        try:
            with self.engine.connect() as db_conn:
                df = pd.read_sql_query(sql=text(query), con=db_conn)
            return df

        except (Exception, exc.SQLAlchemyError) as error:
            print("Error fetching data from PostgreSQL table", error)
            return None

    def table_from_pandas(self, df: pd.DataFrame, table_name: str):
        df.to_sql(name=table_name, con=self.engine, if_exists='append')
    

class InitQueries():
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def create_tablespace(self, tbs_name, tbs_location):
        query = f"CREATE TABLESPACE {tbs_name} 
                  OWNER {self.user} 
                  LOCATION '{tbs_location}'"
        self.db_manager.fetch_result(query)

    def create_database(self, tbs_name, db_name):
        query = f"CREATE DATABASE {db_name} 
                  OWNER {self.user} 
                  TABLESPACE {tbs_name}"
        self.db_manager.fetch_result(query)
        
    def create_table(self, table_name, columns, tbs_name):
        query = f"CREATE TABLE {table_name} ({columns}) 
                  TABLESPACE {tbs_name}"
        self.db_manager.fetch_result(query)
