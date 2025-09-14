import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()
postgres_user = os.environ.get("POSTGRES_USER")
postgres_pass = os.environ.get("POSTGRES_PASSWORD")
postgres_db = os.environ.get("POSTGRES_MAIN_DB")

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    user=postgres_user,
    password=postgres_pass,
    database=postgres_db,
)
cur = conn.cursor()

# CREATING TABLES
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


cur.execute(create_groups_table)
cur.execute(create_courses_table)
cur.execute(create_lecturers_table)
cur.execute(create_students_table)
cur.execute(create_groups_courses_table)
cur.execute(create_courses_lecturers_table)

conn.commit()
cur.close()
conn.close()
