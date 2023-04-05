from .schemas.users_schema import schema
from .utils import get_json


def get_users(**kwargs):
    j = get_json('https://jsonplaceholder.typicode.com/users')

    spark = kwargs.get('session')
    df = spark.createDataFrame(data=j, schema=schema)

    users_filename = 'users.csv'

    df.toPandas().to_csv(users_filename, index=False)
    kwargs['ti'].xcom_push(key='users', value=users_filename)
