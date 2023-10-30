import os
from dotenv import load_dotenv
import time
import logging

# import mlflow
# import mlflow.spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, rank, countDistinct, count
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer


def spark_sql2df(spark, table_name, host, port, user, password, dbname, dialect='postgresql'):
    df = spark.read \
        .format('jdbc') \
        .option('url', f'jdbc:{dialect}://{host}:{port}/{dbname}') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', table_name) \
        .option('user', user).option('password', password) \
        .load()
    return df


def filter_rating_df(rating_df,
                     user_id_col='user_id',
                     item_id_col='anime_id',
                     top_popular=500, min_items=5, max_items=10000):
    rating_df = rating_df.orderBy(user_id_col, item_id_col)

    if top_popular:
        popularity_df = rating_df.groupBy(item_id_col) \
            .agg(count('*').alias('popularity')) \
            .orderBy(col('popularity').desc())

        # Select the top K most popular items
        top_popular_items = popularity_df.limit(top_popular)
        rating_df_filtered = rating_df.join(
            top_popular_items, on=item_id_col, how='inner')
    else:
        rating_df_filtered = rating_df

    # Create a column with the count of items per user and filter the base to select
    # only users within a certain value range of the number of watched titles
    rating_df_filtered = rating_df_filtered.withColumn(
        'num_items', expr('count(*) over (partition by user_id)'))
    rating_df_filtered = rating_df_filtered.filter(
        col('num_items') >= min_items)
    rating_df_filtered = rating_df_filtered.filter(
        col('num_items') <= max_items)
    return rating_df_filtered


def cf_train_test_split(rating_df, test_ratio=0.2,
                        user_id_col='user_id', item_id_col='anime_id', rating_id_col='score',
                        do_index=False,
                        user_index_col='user_index', item_index_col='item_index',):

    # Determine the number of items to mask for each user
    df_rec_final = rating_df.withColumn(
        'num_items_to_mask', (col('num_items') * test_ratio).cast('int'))
    # Masks items for each user
    user_window = Window.partitionBy(
        user_id_col).orderBy(col(item_id_col).desc())
    df_rec_final = df_rec_final.withColumn(
        'item_rank', rank().over(user_window))

    if do_index:
        # Create a StringIndexer model to index the user ID column
        indexer_user = StringIndexer(
            inputCol=user_id_col, outputCol=user_index_col).setHandleInvalid('keep')
        indexer_item = StringIndexer(
            inputCol=item_id_col, outputCol=item_index_col).setHandleInvalid('keep')

        # Fit the indexer model to the data and transform the DataFrame
        df_rec_final = indexer_user.fit(df_rec_final).transform(df_rec_final)
        df_rec_final = indexer_item.fit(df_rec_final).transform(df_rec_final)

        # Convert the user_index column to integer type
        df_rec_final = df_rec_final.withColumn(user_index_col, df_rec_final[user_index_col].cast('integer'))\
            .withColumn(item_index_col, df_rec_final[item_index_col].cast('integer'))

        train_df_rec = df_rec_final.filter(
            col('item_rank') > col('num_items_to_mask')).select(col(user_id_col),
                                                                col(item_id_col),
                                                                col(rating_id_col),
                                                                col(user_index_col),
                                                                col(item_index_col))
        test_df_rec = df_rec_final.filter(
            col('item_rank') <= col('num_items_to_mask')).select(col(user_id_col),
                                                                 col(item_id_col),
                                                                 col(rating_id_col),
                                                                 col(user_index_col),
                                                                 col(item_index_col))
    else:
        train_df_rec = df_rec_final.filter(
            col('item_rank') > col('num_items_to_mask')).select(col(user_id_col), col(item_id_col), col(rating_id_col))
        test_df_rec = df_rec_final.filter(
            col('item_rank') <= col('num_items_to_mask')).select(col(user_id_col), col(item_id_col), col(rating_id_col))
    return train_df_rec, test_df_rec


if __name__ == '__main__':
    load_dotenv()

    dialect = os.getenv('DIALECT')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    # spark_jar_packages = 'org.mlflow:mlflow-spark:1.11.0,./misc/postgresql-42.6.0.jar'
    # spark_jar_packages = 'org.mlflow:mlflow-spark:1.11.0'
    spark_jar_packages = './misc/postgresql-42.6.0.jar'

    spark = SparkSession.builder.config('spark.jars', spark_jar_packages) \
        .master('local[*]').appName('Anime_Recommender').config('spark.driver.memory', '15g').getOrCreate()

    user_df = spark_sql2df(spark, 'mal_user', host,
                           port, user, password, dbname, dialect)
    rating_df = spark_sql2df(spark, 'mal_rating', host,
                             port, user, password, dbname, dialect)

    rating_df_filtered = filter_rating_df(rating_df)
    print(rating_df_filtered.count())
