import json
import os
from datetime import datetime, timedelta

import requests
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

from jinja2 import Environment, FileSystemLoader

S3_FILE_NAME = f"{datetime.today().date()}_top_questions.json"


def call_stack_overflow_api() -> dict:
    """ Get first 100 questions created two days ago sorted by user votes """

    stack_overflow_question_url = Variable.get("STACK_OVERFLOW_QUESTION_URL")

    today = datetime.now()
    three_days_ago = today - timedelta(days=7)
    two_days_ago = today - timedelta(days=5)

    payload = {
        "fromdate": int(datetime.timestamp(three_days_ago)),
        "todate": int(datetime.timestamp(two_days_ago)),
        "sort": "votes",
        "site": "stackoverflow",
        "order": "desc",
        "tagged": Variable.get("TAG"),
        "client_id": Variable.get("STACK_OVERFLOW_CLIENT_ID"),
        "client_secret": Variable.get("STACK_OVERFLOW_CLIENT_SECRET"),
        "key": Variable.get("STACK_OVERFLOW_KEY"),
    }

    response = requests.get(stack_overflow_question_url, params=payload)

    for question in response.json().get("items", []):
        yield {
            "question_id": question["question_id"],
            "title": question["title"],
            "is_answered": question["is_answered"],
            "link": question["link"],
            "owner_reputation": question["owner"].get("reputation", 0),
            "score": question["score"],
            "tags": question["tags"],
        }


def insert_question_to_db():
    """ Inserts a new question to the database """

    insert_question_query = """
        INSERT INTO public.questions (
            question_id,
            title,
            is_answered,
            link,
            owner_reputation, 
            score, 
            tags)
        VALUES (%s, %s, %s, %s, %s, %s, %s); 
        """

    rows = call_stack_overflow_api()
    for row in rows:
        row = tuple(row.values())
        pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
        pg_hook.run(insert_question_query, parameters=row)


def filter_questions() -> str:
    """ 
    Read all questions from the database and filter them.
    Returns a JSON string that looks like:
    
    [
        {
        "title": "Question Title",
        "is_answered": false,
        "link": "https://stackoverflow.com/questions/0000001/...",
        "tags": ["tag_a","tag_b"],
        "question_id": 0000001
        },
    ]
    
    """
    columns = ("title", "is_answered", "link", "tags", "question_id")
    filtering_query = """
        SELECT title, is_answered, link, tags, question_id
        FROM public.questions
        WHERE score >= 1 AND owner_reputation > 1000;
        """
    pg_hook = PostgresHook(postgres_conn_id="postgres_connection").get_conn()

    with pg_hook.cursor("serverCursor") as pg_cursor:
        pg_cursor.execute(filtering_query)
        rows = pg_cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        return json.dumps(results, indent=2)


def write_questions_to_s3():
    hook = S3Hook(aws_conn_id="s3_connection")
    hook.load_string(
        string_data=filter_questions(),
        key=S3_FILE_NAME,
        bucket_name=Variable.get("S3_BUCKET"),
        replace=True,
    )


def render_template(**context):
    """ Render HTML template using questions metadata from S3 bucket """

    hook = S3Hook(aws_conn_id="s3_connection")
    file_content = hook.read_key(
        key=S3_FILE_NAME, bucket_name=Variable.get("S3_BUCKET")
    )
    questions = json.loads(file_content)

    root = os.path.dirname(os.path.abspath(__file__))
    env = Environment(loader=FileSystemLoader(root))
    template = env.get_template("email_template.html")
    html_content = template.render(questions=questions)

    # Push rendered HTML as a string to the Airflow metadata database
    # to make it available for the next task

    task_instance = context["task_instance"]
    task_instance.xcom_push(key="html_content", value=html_content)

