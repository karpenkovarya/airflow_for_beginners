import json
import logging

import requests
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from airflow.models import Variable

# TODO: remove this from here and import from src (leave only DAG definitions)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def call_stack_overflow_api():
    """ Get first 100 questions created in the last 24 hours sorted by user votes. """
    stackoverflow_question_url = Variable.get("STACKOVERFLOW_QUESTION_URL")
    today = datetime.now()
    three_days_ago = today - timedelta(days=3)
    two_days_ago = today - timedelta(days=2)
    tag = "pandas"
    payload = {
        "fromdate": int(datetime.timestamp(three_days_ago)),
        "todate": int(datetime.timestamp(two_days_ago)),
        "sort": "votes",
        "site": "stackoverflow",
        "order": "desc",
        "tagged": tag,
        "pagesize": 100,
        "client_id": Variable.get("STACKOVERFLOW_CLIENT_ID"),
        "client_secret": Variable.get("STACKOVERFLOW_CLIENT_SECRET"),
        "key": Variable.get("STACKOVERFLOW_KEY"),
    }
    response = requests.get(stackoverflow_question_url, params=payload)
    if response.status_code != 200:
        raise Exception(
            f"Cannot fetch questions: {response.status_code} \n {response.json()}"
        )
    for question in response.json().get("items", []):
        yield parse_question(question)


def parse_question(question: dict) -> dict:
    """ Returns parsed question from Stack Overflow API """
    return {
        "question_id": question["question_id"],
        "title": question["title"],
        "is_answered": question["is_answered"],
        "link": question["link"],
        "owner_reputation": question["owner"]["reputation"],
        "score": question["score"],
        "tags": question["tags"],
    }


def add_question():
    """
    Add a new question to the database
    """
    insert_question_query = (
        "INSERT INTO public.questions "
        "(question_id, title, is_answered, link, "
        "owner_reputation, score, "
        "tags) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s);"
    )

    rows = call_stack_overflow_api()
    for row in rows:
        pg_hook = PostgresHook(postgres_conn_id="postgres_so")
        row = tuple(row.values())
        pg_hook.run(insert_question_query, parameters=row)


def generate_file_name():
    now = datetime.now()
    int_timestamp = int(now.timestamp())
    return f"{int_timestamp}_top_questions.json"


def filter_questions():
    filtering_query = """
        SELECT title, is_answered, link, tags, question_id
        FROM public.questions
        WHERE score >= 1 AND owner_reputation > 1000;
        """
    pg_hook = PostgresHook(postgres_conn_id="postgres_so").get_conn()

    with pg_hook.cursor("serverCursor") as src_cursor:
        src_cursor.execute(filtering_query)
        rows = src_cursor.fetchall()
        columns = ("title", "is_answered", "link", "tags", "question_id")
        results = []
        for row in rows:
            record = dict(zip(columns, row))
            results.append(record)

        return results


def write_questions_to_s3(**context):
    filtered_questions = json.dumps(filter_questions(), indent=2)

    file_name = generate_file_name()
    json.dumps(filtered_questions, indent=2)
    hook = S3Hook("s3_connection")
    hook.load_string(
        string_data=filtered_questions,
        key=file_name,
        bucket_name="stack.overflow.questions",
    )
    task_instance = context["task_instance"]
    task_instance.xcom_push(key="file_name", value=file_name)


def read_json_from_s3(**context):
    value = context["task_instance"].xcom_pull(
        task_ids="write_questions_to_s3", key="file_name"
    )
    hook = S3Hook("s3_connection")
    file_content = hook.read_key(key=value, bucket_name="stack.overflow.questions")
    questions = json.loads(file_content)
    template_header = f"The top {len(questions)} questions under the tag 'pandas' on {datetime.today().date()}\n\n"
    template_body = ""
    for q_number, question in enumerate(questions, start=1):
        template_body += f"{q_number}. {question['title'].capitalize()} \ntagged: {', '.join(question['tags'])} \n{question['link']} \n\n"

    return template_header + template_body


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 6, 1),
    "email": ["airflow@varya.io"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "tutorial", default_args=default_args, schedule_interval=timedelta(days=1)
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
        task_id="read_json_from_s3",
        python_callable=read_json_from_s3,
        dag=dag,
        provide_context=True,
    )
    t5 = EmailOperator(
        task_id='send_email',
        to='karpenko.varya@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
    )

t1 >> t2 >> t3 >> t4 >> t5
