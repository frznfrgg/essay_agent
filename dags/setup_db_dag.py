import os
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv


def create_tables():
    load_dotenv()
    pg_hook = PostgresHook(
        host="postgres",
        schema=os.getenv("POSTGRES_MAIN_DB"),
        login=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=5432,
    )
    create_students_table = """
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            group_code VARCHAR(50) NOT NULL REFERENCES groups(code),
            essay_text TEXT NOT NULL,
            CONSTRAINT unique_student_full_name UNIQUE (first_name, last_name)
        );
    """

    create_groups_table = """
        CREATE TABLE IF NOT EXISTS groups (
            code VARCHAR(50) PRIMARY KEY,
            graduation_date DATE
        );
    """

    create_courses_table = """
        CREATE TABLE IF NOT EXISTS courses (
            id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name VARCHAR(200) NOT NULL UNIQUE
        );
    """

    create_lecturers_table = """
        CREATE TABLE IF NOT EXISTS lecturers (
            id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            CONSTRAINT unique_lecturer_full_name UNIQUE (first_name, last_name)
        );
    """

    create_courses_lecturers_table = """
        CREATE TABLE IF NOT EXISTS courses_lecturers (
            course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
            lecturer_id INTEGER NOT NULL REFERENCES lecturers(id) ON DELETE CASCADE,
            PRIMARY KEY(course_id, lecturer_id)
        );
    """

    create_groups_courses_table = """
        CREATE TABLE IF NOT EXISTS groups_courses (
            group_code VARCHAR(50) NOT NULL REFERENCES groups(code) ON DELETE CASCADE,
            course_id INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
            PRIMARY KEY(group_code, course_id)
        );
    """
    pg_hook.run(create_groups_table)
    pg_hook.run(create_courses_table)
    pg_hook.run(create_lecturers_table)
    pg_hook.run(create_students_table)
    pg_hook.run(create_groups_courses_table)
    pg_hook.run(create_courses_lecturers_table)


def fill_tables():
    import pandas as pd

    load_dotenv()
    courses_lecturers_path = os.getenv("COURSES_LECTURERS_CSV")
    pg_hook = PostgresHook(
        host="postgres",
        schema=os.getenv("POSTGRES_MAIN_DB"),
        login=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=5432,
    )

    courses_lecturers_df = pd.read_csv(
        courses_lecturers_path,  # type:ignore
        names=["Courses", "Lecturers"],
    )
    courses_lecturers_df["Lecturers"] = courses_lecturers_df["Lecturers"].apply(
        lambda lecturers: [name.split() for name in lecturers.split("\n")]
    )
    # inserting lecturers
    for lecturers in courses_lecturers_df["Lecturers"]:
        for lecturer in lecturers:
            name, surname = lecturer[0], " ".join(lecturer[1:])
            pg_hook.run(
                """
                    INSERT INTO lecturers(first_name, last_name) VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;
                """,
                parameters=(name, surname),
            )

    # inserting courses
    for course in courses_lecturers_df["Courses"]:
        pg_hook.run(
            """
                INSERT INTO courses(name) VALUES (%s)
                ON CONFLICT DO NOTHING;
            """,
            parameters=(course,),
        )

    # inserting junction table courses_lecturers
    rows = []
    for _, row in courses_lecturers_df.iterrows():
        course = row["Courses"]
        for lec in row["Lecturers"]:
            first_name, last_name = lec[0], " ".join(lec[1:])
            rows.append((course, first_name, last_name))

    for course, first_name, last_name in rows:
        course_id = pg_hook.get_first(
            "SELECT id FROM courses WHERE name=%s", parameters=(course,)
        )[0]
        lecturer_id = pg_hook.get_first(
            "SELECT id FROM lecturers WHERE first_name=%s AND last_name=%s",
            parameters=(first_name, last_name),
        )[0]
        pg_hook.run(
            "INSERT INTO courses_lecturers (course_id, lecturer_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            parameters=(course_id, lecturer_id),
        )


with DAG(
    dag_id="setup_db_dag",
    start_date=datetime(2025, 9, 14),
    schedule="@once",
    catchup=False,
) as dag:
    create = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )
    fill = PythonOperator(
        task_id="fill_tables",
        python_callable=fill_tables,
    )

    create >> fill  # type:ignore
