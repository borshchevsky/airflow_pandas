import logging

import pyspark.sql.functions as f

from .utils import get_json


def get_posts(**kwargs):
    spark = kwargs.get('session')
    users_csv = kwargs['ti'].xcom_pull(key='users')
    users_df = spark.read.csv(users_csv, header=True)

    user_ids = users_df.select(f.collect_list('id')).first()[0]

    posts_count = []
    for user_id in user_ids:
        url = f'https://jsonplaceholder.typicode.com/users/{user_id}/posts'
        j = get_json(url)
        logging.info(f'Got posts information for user # {user_id}')
        posts_count.append({'id': user_id, 'post_count': len(j)})

    posts_filename = 'posts.csv'
    posts_count_df = spark.createDataFrame(data=posts_count)
    posts_count_df.toPandas().to_csv('posts.csv', index=False)

    kwargs['ti'].xcom_push(key='posts', value=posts_filename)
