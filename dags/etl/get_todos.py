import logging

import pyspark.sql.functions as f

from .utils import get_json


def get_todos(**kwargs):
    spark = kwargs.get('session')
    users_csv = kwargs['ti'].xcom_pull(key='users')
    users_df = spark.read.csv(users_csv, header=True)

    user_ids = users_df.select(f.collect_list('id')).first()[0]

    todos_count = []
    for user_id in user_ids:
        url = f'https://jsonplaceholder.typicode.com/users/{user_id}/todos'
        j = get_json(url)
        logging.info(f'Got todos information for user # {user_id}')
        todos_count.append({'id': user_id, 'todo_count': len(j)})

    todos_filename = 'todos.csv'
    todos_count_df = spark.createDataFrame(data=todos_count)
    todos_count_df.toPandas().to_csv('todos.csv', index=False)

    kwargs['ti'].xcom_push(key='todos', value=todos_filename)
