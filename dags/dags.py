from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from dags.utils import insert_question, write_questions_to_s3, render_template


default_args = {
    "owner": "varya",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 9),
    "email": ["karpenko.varya@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "stack_overflow_questions", default_args=default_args, schedule_interval="@daily"
) as dag:

    Task_I = PostgresOperator(
        dag=dag,
        task_id="create_table",
        postgres_conn_id="postgres_connection",
        database="stack_overflow",
        sql="""
        DROP TABLE IF EXISYS public.questions;
        CREATE TABLE public.questions
        (
            title text,
            is_answered boolean,
            link character varying,
            score integer,
            tags text[],
            question_id integer NOT NULL,
            owner_reputation integer
        )
        """,
    )

    Task_II = PythonOperator(
        dag=dag, task_id="insert_questions", python_callable=insert_question
    )

    Task_III = PythonOperator(
        dag=dag, task_id="write_questions_to_s3", python_callable=write_questions_to_s3
    )

    Task_IV = PythonOperator(
        dag=dag,
        task_id="render_template",
        python_callable=render_template,
        provide_context=True,
    )

    Task_V = EmailOperator(
        dag=dag,
        task_id="send_email",
        provide_context=True,
        to="hello@varya.io",
        subject="Top questions with tag 'pandas' on {{ ds }}",
        html_content="{{ task_instance.xcom_pull(task_id='render_template', key='html_content') }}"
    )

Task_I >> Task_II >> Task_III >> Task_IV >> Task_V 

