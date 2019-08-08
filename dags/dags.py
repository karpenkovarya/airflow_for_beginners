from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator

from airflow.hooks.postgres_hook import PostgresHook


def add_question():
    """
    Add a new question to the database
    """
    insert_question_query = '''INSERT INTO public.questions(
	title, is_answered, link, owner_reputation, score, tags, question_id)
	VALUES (%s, %s, %s, %s, %s, %s, %s);'''

    title = "Test Insert 2"
    is_answered = False
    link = "http://www.postgresqltutorial.com/postgresql-python/insert/"
    owner_reputation = 100
    score = 200
    tags = ['postgres']
    question_id=2
    pg_hook = PostgresHook(postgres_conn_id="postgres_so")
    row = (title, is_answered, link, owner_reputation, score, tags, question_id)
    pg_hook.run(insert_question_query, parameters=row)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('tutorial', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
    t1 = PythonOperator(
        task_id='into_db',
        python_callable=add_question,
        dag=dag)

    t2 = BashOperator(
        task_id='print_date',
        bash_command='date',
        dag=dag)

t1 >> t2
