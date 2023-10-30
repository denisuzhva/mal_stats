from dotenv import load_dotenv
import os
import gc
import logging
import configparser

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import expr

from etl.csv_to_db import etl_main
from etl.db_manager import DatabaseManager
from ml_pipeline import ml_pipeline


RND_SEED = 621


def setup_logger(fmt='[%(levelname)s] %(asctime)s - %(message)s', log_path='./log/rsys.log'):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt)
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(formatter)

    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    logger.addHandler(sh)
    logger.addHandler(fh)

    return logger


if __name__ == '__main__':
    logger = setup_logger()

    config = configparser.ConfigParser()
    config.read('cfg/baseconfig.ini')

    user_table_name = config['ETL']['user_table_name']
    item_table_name = config['ETL']['item_table_name']
    rating_table_name = config['ETL']['rating_table_name']
    user_id_col = config['ETL']['user_id_col']
    item_id_col = config['ETL']['item_id_col']
    rating_col = config['ETL']['rating_col']
    do_filter = config.getboolean('ETL', 'do_filter')
    sample_fraction = config.getfloat('ETL', 'sample_fraction')
    top_popular = config.getint('ETL', 'top_popular')
    min_items = config.getint('ETL', 'min_items')
    max_items = config.getint('ETL', 'max_items')

    user_index_col = config['RSYS']['user_index_col']
    item_index_col = config['RSYS']['item_index_col']

    load_dotenv()
    dialect = os.getenv('DIALECT')
    host = os.getenv('HOST')
    port = os.getenv('PORT')
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')
    datapath = os.getenv('DATAPATH')

    db_manager = DatabaseManager(host, port, user, password, dbname)
    logger.debug('ETL')
    if config.getboolean('ETL', 'do_etl'):
        if do_filter:
            filter_params = {'sample_fraction': sample_fraction, 'top_popular': top_popular,
                             'min_items': min_items, 'max_items': max_items}
        else:
            filter_params = None
        etl_main(db_manager, do_extract=False,
                 data_dir=datapath,
                 user_fields=None,
                 item_fields=None,
                 rating_fields=None,
                 filter_params=filter_params,
                 user_table_name=user_table_name,
                 item_table_name=item_table_name,
                 rating_table_name=rating_table_name,
                 user_id_col=user_id_col,
                 item_id_col=item_id_col,
                 rating_col=rating_col,)

    if config.getboolean('RSYS', 'train_rsys'):
        logger.debug('RSYS setup')
        spark_jar_packages = config['RSYS']['spark_jar_packages']
        spark = SparkSession.builder.config("spark.jars", spark_jar_packages) \
            .master("local[*]").appName("Anime_Recommender") \
            .config("spark.driver.memory", "15g") \
            .getOrCreate()

        user_df = ml_pipeline.spark_sql2df(spark, user_table_name, host,
                                           port, user, password, dbname, dialect)
        rating_table_f_name = f'{rating_table_name}_filtered'
        rating_df = ml_pipeline.spark_sql2df(spark, rating_table_f_name, host,
                                             port, user, password, dbname, dialect)

        logger.debug('Rating train test split')
        train_df, test_df = ml_pipeline.cf_train_test_split(
            rating_df, user_id_col=user_id_col, item_id_col=item_id_col,
            do_index=True, user_index_col=user_index_col, item_index_col=item_index_col)

        als = ALS(userCol=user_index_col, itemCol=item_index_col, ratingCol=rating_col,
                  coldStartStrategy='drop', nonnegative=True)

        param_grid = ParamGridBuilder()\
            .addGrid(als.rank, [1, 5, 10])\
            .addGrid(als.maxIter, [10])\
            .addGrid(als.regParam, [.05, .1])\
            .build()
        evaluator = RegressionEvaluator(
            metricName='rmse', labelCol=rating_col, predictionCol='prediction')

        cv = CrossValidator(
            estimator=als,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3)

        spark.sparkContext._jvm.System.gc()
        gc.collect()
        logger.debug('Model fitting')
        model = cv.fit(train_df)
        logger.debug('Model fitted')

        best_model = model.bestModel
        print('rank: ', best_model.rank)
        print('MaxIter: ', best_model._java_obj.parent().getMaxIter())
        print('RegParam: ', best_model._java_obj.parent().getRegParam())

        # Generate predictions on the test data
        predictions = best_model.transform(test_df)
        predictions = predictions.withColumn("prediction", expr(
            "CASE WHEN prediction < 1 THEN 1 WHEN prediction > 10 THEN 10 ELSE prediction END"))

        evaluator = RegressionEvaluator(
            metricName='rmse', labelCol='score', predictionCol='prediction')
        rmse = evaluator.evaluate(predictions)
        print(f'Root Mean Squared Error (RMSE): {rmse}')
