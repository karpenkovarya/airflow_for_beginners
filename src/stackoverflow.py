import json
import os

import psycopg2
import requests
from datetime import datetime, timedelta
from pprint import pprint

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from psycopg2.extras import RealDictCursor


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
        "creation_date": creation_date,
    }


def call_stack_overflow_api():
    """ Get first 100 questions created in the last 24 hours sorted by user activity. """
    stackoverflow_question_url = Variable.get("STACKOVERFLOW_QUESTION_URL")
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
        "client_id": Variable.get("STACKOVERFLOW_CLIENT_ID"),
        "client_secret": Variable.get("STACKOVERFLOW_CLIENT_SECRET"),
        "key": Variable.get("STACKOVERFLOW_KEY"),
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
    insert_question_query = (
        "INSERT INTO public.questions "
        "(question_id, title, is_answered, link, "
        "owner_reputation, owner_accept_rate, score, "
        "tags, creation_date) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s);"
    )

    rows = call_stack_overflow_api()
    for row in rows:
        row = tuple(row.values())
        pg_hook = PostgresHook(postgres_conn_id="postgres_so")
        pg_hook.run(insert_question_query, parameters=row)


if __name__ == "__main__":
    pass
