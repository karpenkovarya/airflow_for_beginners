import os

import psycopg2
import requests
from datetime import datetime, timedelta
from pprint import pprint

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable


class ApiError(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def parse_question(question: dict) -> dict:
    """ Returns parsed question from Stack Overflow API """
    creation_date = datetime.fromtimestamp(question["creation_date"])
    return {
        "question_id": question["question_id"],
        "title": question["title"],
        "is_answered": question["is_answered"],
        "link": question["link"],
        "owner_reputation": question["owner"]["reputation"],
        "owner_accept_rate": question["owner"].get("accept_rate", 0),
        "score": question["score"],
        "tags": question["tags"],
        "creation_date": creation_date
    }


def call_stack_overflow_api():
    """ Get first 100 questions created in the last 24 hours sorted by user activity. """
    # stackoverflow_question_url = Variable.get("STACKOVERFLOW_QUESTION_URL")
    stackoverflow_question_url = "https://api.stackexchange.com/2.2/questions"
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    tag = "django"
    payload = {
        "fromdate": int(datetime.timestamp(yesterday)),
        "todate": int(datetime.timestamp(today)),
        "sort": "activity",
        "site": "stackoverflow",
        "order": "desc",
        "tagged": tag,
        "pagesize": 100,
        "client_id": os.environ["SO_CLIENT_ID"], #Variable.get("STACKOVERFLOW_CLIENT_ID"),
        "client_secret": os.environ["SO_CLIENT_SECRET"], #Variable.get("STACKOVERFLOW_CLIENT_SECRET"),
        "key": os.environ["SO_KEY"], #Variable.get("STACKOVERFLOW_KEY"),
    }
    response = requests.get(stackoverflow_question_url, params=payload)
    if response.status_code != 200:
        raise ApiError(
            f"Cannot fetch questions: {response.status_code} \n {response.json()}"
        )
    for question in response.json().get("items", []):
        yield parse_question(question)

def add_question():
    """
    Add a new question to the database
    """
    insert_question_query = ("INSERT INTO public.questions "
                             "(question_id, title, is_answered, link, "
                             "owner_reputation, owner_accept_rate, score, "
                             "tags, creation_date) "
                             "VALUES (%s, %s, %s, %s, %s, %s, %s);")


    rows = call_stack_overflow_api()
    for row in rows:
        row = tuple(row.values())


        pg_hook = PostgresHook(postgres_conn_id="postgres_so")
        pg_hook.run(insert_question_query, parameters=row)

def add_question_locally():
    """
    Add a new question to the database
    """
    insert_question_query = ("INSERT INTO public.questions "
                             "(question_id, title, is_answered, link, "
                             "owner_reputation, owner_accept_rate, score, "
                             "tags, creation_date) "
                             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);")

    conn = psycopg2.connect(user = "postgres",
                                  password = "4mygdala!",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "stack_overflow")
    cur = conn.cursor()
    rows = call_stack_overflow_api()
    for row in rows:
        row = tuple(row.values())
        cur.execute(insert_question_query, row)
        cur.close()
        conn.commit()
        conn.close()
        break

if __name__ == "__main__":

    add_question_locally()
