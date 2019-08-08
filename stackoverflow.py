import requests
import os
from datetime import datetime, timedelta
from pprint import pprint
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


def call_so():
    BASE_URL = "https://api.stackexchange.com/2.2/questions"
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
        "pagesize":100,
        "client_id": os.environ["SO_CLIENT_ID"],
        "client_secret": os.environ["SO_CLIENT_SECRET"],
        "key": os.environ["SO_KEY"],
    }
    r = requests.get(BASE_URL, params=payload).json()
    print(r.keys())
    print(len(r['items']))
    print(r['has_more'])
    pprint(r['items'][:5])

    return r


if __name__ == "__main__":

    add_question()
