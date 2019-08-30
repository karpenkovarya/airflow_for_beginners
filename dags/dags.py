
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from utils import write_questions_to_s3, add_question, render_template_and_send_email


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 30),
    "email": ["karpenko.varya@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "pandas_questions", default_args=default_args, schedule_interval=timedelta(days=1)
) as dag:
    t1 = PostgresOperator(
        task_id="truncate_questions_table",
        postgres_conn_id="postgres_so",
        sql="TRUNCATE TABLE public.questions",
        database="stack_overflow",
        dag=dag,
    )

    t2 = PythonOperator(
        task_id="insert_questions_into_db", python_callable=add_question, dag=dag
    )
    t3 = PythonOperator(
        task_id="write_questions_to_s3",
        python_callable=write_questions_to_s3,
        dag=dag,
        provide_context=True,
    )
    t4 = PythonOperator(
        task_id="render_template",
        python_callable=render_template_and_send_email,
        dag=dag,
        provide_context=True,
    )

    t5 = EmailOperator(
        task_id="send_email",
        provide_context=True,
        dag=dag,
        to="karpenko.varya@gmail.com",
        subject = f"Top questions with tag 'pandas' on {{ds}}",
        html_content = "{{ task_instance.xcom_pull(task_ids='render_template', key='html_content') }}"
    )

t1 >> t2 >> t3 >> t4 >> t5
