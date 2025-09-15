import os
import shutil
from datetime import datetime

import docx
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator

WATCH_DIR = "/opt/airflow/data/incoming"


def list_subfolder(**context):
    """Find subfolder passed by trigger"""
    conf = context["dag_run"].conf
    subfolder = conf.get("subfolder")
    if not subfolder:
        raise ValueError('You must trigger DAG with {"subfolder": "GroupName"}')
    folder_path = os.path.join(WATCH_DIR, subfolder)
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Subfolder {folder_path} not found")

    files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.endswith(".docx")
    ]
    context["ti"].xcom_push(key="group_name", value=subfolder)
    context["ti"].xcom_push(key="files", value=files)


def parse_docs(**context):
    files = context["ti"].xcom_pull(key="files", task_ids="list_subfolder")
    group_name = context["ti"].xcom_pull(key="group_name", task_ids="list_subfolder")

    students = []
    for file in files:
        fname = os.path.basename(file).replace(".docx", "")
        try:
            first, last = fname.split("_", 1)
        except ValueError:
            raise ValueError(
                f"Invalid filename format: {fname}. Expected Name_Surname.docx"
            )

        doc = docx.Document(file)
        text = "\n".join([p.text for p in doc.paragraphs])
        students.append(
            {"group": group_name, "first": first, "last": last, "text": text}
        )

    context["ti"].xcom_push(key="students", value=students)


def insert_postgres(**context):
    students = context["ti"].xcom_pull(key="students", task_ids="parse_docs")

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    insert_sql = """
        INSERT INTO students (group_code, first_name, last_name, essay_text)
        VALUES (%s, %s, %s, %s)
    """
    for s in students:
        pg_hook.run(
            insert_sql, parameters=(s["group"], s["first"], s["last"], s["text"])
        )


def cleanup_folder(**context):
    group_name = context["ti"].xcom_pull(key="group_name", task_ids="list_subfolder")
    folder_path = os.path.join(WATCH_DIR, group_name)
    if os.path.isdir(folder_path):
        shutil.rmtree(folder_path)


with DAG(
    dag_id="process_student_docs",
    start_date=datetime(2025, 9, 14),
    schedule=None,
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="list_subfolder",
        python_callable=list_subfolder,
    )

    t2 = PythonOperator(
        task_id="parse_docs",
        python_callable=parse_docs,
    )

    t3 = PythonOperator(
        task_id="insert_postgres",
        python_callable=insert_postgres,
    )

    t4 = PythonOperator(
        task_id="cleanup_folder",
        python_callable=cleanup_folder,
    )

    t1 >> t2 >> t3 >> t4  # type:ignore
