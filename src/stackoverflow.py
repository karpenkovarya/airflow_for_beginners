import requests
from datetime import datetime, timedelta
from pprint import pprint
from airflow.models import Variable


class ApiError(Exception):
    def __init__(self,*args,**kwargs):
        Exception.__init__(self,*args,**kwargs)


def parse_question(question: dict) -> dict:
    """ Returns parsed question from Stack Overflow API """

    return {
        "question_id": question['question_id'],
        "title": question['question_id'],
        "is_answered": question['is_answered'],
        "link": question['link'],
        "owner_reputation": question['owner']['reputation'],
        "owner_accept_rate": question['owner']['accept_rate'],
        "score": question['score'],
        "tags": question['tags'],
        "creation_date": question['creation_date']
    }


def call_stack_overflow_api():
    """ Get first 100 questions created in the last 24 hours sorted by user activity. """
    stackoverflow_question_url = Variable.get("STACKOVERFLOW_QUESTION_URL")
    # stackoverflow_question_url = "https://api.stackexchange.com/2.2/questions"
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
        "client_id": Variable.get("STACKOVERFLOW_CLIENT_ID"), #os.environ["SO_CLIENT_ID"],
        "client_secret": Variable.get("STACKOVERFLOW_CLIENT_SECRET"),
        "key": Variable.get("STACKOVERFLOW_KEY"),
    }
    response = requests.get(stackoverflow_question_url, params=payload)
    if response.status_code != 200:
        raise ApiError(f'Cannot fetch questions: {response.status_code} \n {response.json()}')
    for question in response.json().get("items", []):
        yield parse_question(question)


if __name__ == "__main__":

    a = call_stack_overflow_api()
    pprint(next(a))
