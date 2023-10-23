from sqlalchemy import create_engine, exc, text
from io import StringIO
import pandas as pd


def pd_to_sql_dtypes(dtype_list):
    data_list = []
    for x in dtype_list:
        if (x == 'int64'):
            data_list.append('int')
        elif (x == 'float64'):
            data_list.append('float')
        elif (x == 'bool'):
            data_list.append('boolean')
        else:
            data_list.append('varchar')
    return data_list


def df_to_query_col_string(df):
    mapped_dtypes = pd_to_sql_dtypes(df.dtypes)
    col_names = list(df.columns.values)
    query_col_string = '\n '.join('{col_name} {col_type},'.format(
        col_name=col_name, col_type=col_type) for col_name, col_type in zip(col_names, mapped_dtypes))[:-1]
    return query_col_string


class DatabaseManager():
    def __init__(self,
                 host: str,
                 port: str,
                 user: str,
                 password: str,
                 dbname: str = '',
                 dialect: str = 'postgresql',
                 driver: str = 'psycopg2'):
        self.user = user
        self.engine = create_engine(
            '{dialect}+{driver}://{username}:{password}@{host}:{port}'.format(
                dialect=dialect,
                driver=driver,
                username=user,
                password=password,
                host=host,
                port=port,
                fast_executemany=True,
            ) + '/' + dbname
        )

    def execute_query(self, query: str, autocommit: bool = False) -> list:
        try:
            with self.engine.connect() as db_conn:
                if autocommit:
                    q_out = db_conn.execution_options(
                        isolation_level='AUTOCOMMIT').execute(text(query))
                else:
                    q_out = db_conn.execute(text(query))
                df = pd.DataFrame(q_out)
                df.columns = q_out.keys()
                # df = pd.read_sql_query(sql=text(query), con=db_conn)
            return df
        except (Exception, exc.SQLAlchemyError) as error:
            print("Error fetching data from PostgreSQL table", error)
            return None

    def pandas_to_sql(self,
                      df: pd.DataFrame,
                      table_name: str,
                      if_exists: str = 'append',
                      index: bool = False):
        try:
            df.to_sql(name=table_name, con=self.engine,
                      if_exists=if_exists, index=index)
        except ValueError as e:
            return e

    def pandas_to_sql_bulk_postgres(self, df: pd.DataFrame, table_name: str):
        try:
            query_col_string = df_to_query_col_string(df)
            self.create_table(table_name, query_col_string)
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            # contents = output.getvalue()
            conn = self.engine.raw_connection()
            with conn.cursor() as cur:
                cur.copy_from(output, table_name, null="")
                conn.commit()
        except ValueError as e:
            return e

    def create_tablespace(self, tbs_name, tbs_location):
        query = f"""
            DROP TABLESPACE IF EXISTS {tbs_name};
            CREATE TABLESPACE {tbs_name}
            OWNER {self.user} 
            LOCATION '{tbs_location}'
        """
        self.execute_query(query, autocommit=True)

    def create_database(self, tbs_name, db_name):
        query = f"""
            DROP DATABASE IF EXISTS {db_name};
            CREATE DATABASE {db_name} 
            OWNER {self.user} 
            TABLESPACE {tbs_name}
        """
        self.execute_query(query, autocommit=True)

    def create_table(self, table_name, columns):
        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} ({columns}) 
        """
        self.execute_query(query, autocommit=True)

    def drop_table(self, table_name):
        query = f"""
            DROP TABLE IF EXISTS {table_name}
        """
        self.execute_query(query, autocommit=True)
