import pandas as pd


def load(**kwargs):
    users_csv = kwargs['ti'].xcom_pull(key='users')
    posts_csv = kwargs['ti'].xcom_pull(key='posts')
    todos_csv = kwargs['ti'].xcom_pull(key='todos')

    users_df = pd.read_csv(users_csv)
    posts_df = pd.read_csv(posts_csv)
    todos_df = pd.read_csv(todos_csv)

    df = users_df.merge(posts_df).merge(todos_df)

    print(df.to_string(index=False))
