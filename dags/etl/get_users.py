import pandas as pd

from .utils import get_json


def get_users(**kwargs):
    j = get_json('https://jsonplaceholder.typicode.com/users')
    df = pd.DataFrame.from_dict(j).drop(columns=['address', 'phone', 'website', 'company'])
    users_filename = 'users.csv'
    df.to_csv('users.csv', index=False)
    kwargs['ti'].xcom_push(key='users', value=users_filename)
