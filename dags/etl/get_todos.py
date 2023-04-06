import logging

import pandas as pd

from .utils import get_json


def get_todos(**kwargs):
    users_csv = kwargs['ti'].xcom_pull(key='users')
    users_df = pd.read_csv(users_csv)

    todos_count = []
    for user_id in users_df.id.to_list():
        url = f'https://jsonplaceholder.typicode.com/users/{user_id}/todos'
        j = get_json(url)
        logging.info(f'Got todos information for user # {user_id}')
        todos_count.append({'id': user_id, 'task_count': len(j)})

    todos_filename = 'todos.csv'
    todos_df = pd.DataFrame(todos_count)
    todos_df.to_csv(todos_filename, index=False)

    kwargs['ti'].xcom_push(key='todos', value=todos_filename)
