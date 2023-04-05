from .schemas.users_schema import schema


def load(**kwargs):
    spark = kwargs.get('session')
    users_csv = kwargs['ti'].xcom_pull(key='users')
    posts_csv = kwargs['ti'].xcom_pull(key='posts')
    todos_csv = kwargs['ti'].xcom_pull(key='todos')

    users_df = spark.read.csv(users_csv, header=True, schema=schema)
    posts_df = spark.read.csv(posts_csv, header=True)
    todos_df = spark.read.csv(todos_csv, header=True)

    df = users_df.join(posts_df, ['id']).join(todos_df, ['id']).orderBy('id')

    df.show()
