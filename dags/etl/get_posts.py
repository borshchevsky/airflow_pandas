import logging

import pandas as pd

from .utils import get_json


def get_posts(**kwargs):
    users_csv = kwargs['ti'].xcom_pull(key='users')
    users_df = pd.read_csv(users_csv)

    posts_count = []
    for user_id in users_df.id.to_list():
        url = f'https://jsonplaceholder.typicode.com/users/{user_id}/posts'
        j = get_json(url)
        logging.info(f'Got posts information for user # {user_id}')
        posts_count.append({'id': user_id, 'post_count': len(j)})

    posts_filename = 'posts.csv'
    posts_df = pd.DataFrame(posts_count)
    posts_df.to_csv(posts_filename, index=False)

    kwargs['ti'].xcom_push(key='posts', value=posts_filename)
