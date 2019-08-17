[How to create a new Postgres connection in PgAdmin](https://docs.bitnami.com/installer/apps/canvaslms/administration/configure-pgadmin/)

Use `psql -U postgres` to connect to postgres

Create table query:
```sql
CREATE TABLE public.questions
(
    title text,
    is_answered boolean,
    link character varying,
    owner_reputation integer,
    score integer,
    tags text[],
    question_id integer,
    PRIMARY KEY (question_id)
);
```
postgres connection string `airflow_for_beginners_demo://postgres:{your_password}@127.0.0.1:5432/stack_overflow`
[postgres hook tutorial](http://michael-harmon.com/blog/AirflowETL.html)

#### Set up Airflow 
Activate your virtual environment
```
pip install apache-airflow
export AIRFLOW_HOME=`pwd`

```

stackexchange_question_url: "https://api.stackexchange.com/2.2/questions"

setup emil smtp server http://manojvsj.blogspot.com/2016/09/how-smtp-works-and-how-to-configure.html