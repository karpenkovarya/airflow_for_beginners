from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator

from src.stackoverflow import call_stack_overflow_api


def add_question():
    """
    Add a new question to the database
    """
    insert_question_query = """INSERT INTO public.questions(
	 question_id, title, is_answered, link, owner_reputation, owner_accept_rate, score, tags, creation_date)
	VALUES (%s, %s, %s, %s, %s, %s, %s);"""

    row = call_stack_overflow_api()

    pg_hook = PostgresHook(postgres_conn_id="postgres_so")
    row = tuple(row.values())
    pg_hook.run(insert_question_query, parameters=row)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 6, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "tutorial", default_args=default_args, schedule_interval=timedelta(days=1)
) as dag:
    t1 = PythonOperator(task_id="into_db", python_callable=add_question, dag=dag)

    t2 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

t1 >> t2
