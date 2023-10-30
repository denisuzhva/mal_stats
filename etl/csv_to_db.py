import os
import logging
import gc
import pandas as pd

from kaggle.api.kaggle_api_extended import KaggleApi


def etl_main(db_manager,
             do_extract=False,
             ds_owner='svanoo',
             ds_name='myanimelist-dataset',
             data_dir='./data/',
             user_fields=['user_id', 'user_url'],
             item_fields=['anime_id', 'anime_url', 'title', 'main_pic'],
             rating_fields=['user_id', 'anime_id', 'score', 'status'],
             filter_params=None,
             user_table_name='mal_user',
             item_table_name='mal_item',
             rating_table_name='mal_rating',
             user_id_col='user_id',
             item_id_col='anime_id',
             rating_col='score',):

    # Extract all
    if do_extract:
        logging.info("Extracting data...")
        api = KaggleApi()
        api.authenticate()
        os.makedirs(data_dir, exist_ok=True)
        api.dataset_download_files(
            f'{ds_owner}/{ds_name}', path=os.path.join(data_dir), unzip=False)
        logging.info("Dataset extracted")
    else:
        logging.info("Dataset already extracted")

    # Transform and load users
    if user_fields:
        user_df = pd.read_csv(os.path.join(
            data_dir, 'user.csv'), delimiter='\t', usecols=user_fields)
        user_df.rename(columns={user_id_col: 'user_name'}, inplace=True)
        user_df = user_df.rename_axis(user_id_col).reset_index()
        db_manager.drop_table(user_table_name)
        verror = db_manager.pandas_to_sql_bulk_postgres(
            user_df, user_table_name)
        logging.info(
            verror if verror else f"{user_table_name} table processed and loaded")
    else:
        logging.info("No user fields to process")

    # Transform and load items
    if item_fields:
        item_df = pd.read_csv(os.path.join(
            data_dir, 'anime.csv'), delimiter='\t', usecols=item_fields)
        db_manager.drop_table(item_table_name)
        verror = db_manager.pandas_to_sql_bulk_postgres(
            item_df, item_table_name)
        logging.info(
            verror if verror else f"{item_table_name} table processed and loaded")
    else:
        logging.info("No item fields to process")

    # Transform and load ratings
    if rating_fields:
        rating_paths = [path for path in os.listdir(
            data_dir) if 'user_anime' in path]
        db_manager.drop_table(rating_table_name)
        for chunk_id, path in enumerate(rating_paths):
            rating_df = pd.read_csv(os.path.join(data_dir, path),
                                    delimiter='\t',
                                    usecols=rating_fields)
            rating_df = rating_df.loc[rating_df['status']
                                      == 'completed'].dropna()
            rating_df.rename(columns={user_id_col: 'user_name'}, inplace=True)
            rating_df = rating_df.astype({rating_col: int})
            rating_df = rating_df.join(
                user_df.set_index('user_name'), on='user_name')[[user_id_col, item_id_col, rating_col]]
            verror = db_manager.pandas_to_sql_bulk_postgres(
                rating_df, rating_table_name)
            logging.info(
                verror if verror else f"{rating_table_name} chunk #{chunk_id} processed and loaded")
            del rating_df
            gc.collect()
    else:
        logging.info("No rating fields to process")

    if filter_params:
        rating_f_table_name = f'{rating_table_name}_filtered'
        db_manager.drop_table(rating_f_table_name)
        db_manager.popular_minmax_filter(
            rating_table_name, rating_f_table_name, user_id_col, item_id_col, rating_col, **filter_params)
        logging.info(f"{rating_table_name} filtered")
    else:
        logging.info("No filter tasks to perform")
