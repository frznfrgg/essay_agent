import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()
postgres_user = os.environ.get("POSTGRES_USER")
postgres_pass = os.environ.get("POSTGRES_PASSWORD")
postgres_db = os.environ.get("POSTGRES_MAIN_DB")
courses_lecturers_path = os.environ.get("COURSES_LECTURERS_CSV")

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    user=postgres_user,
    password=postgres_pass,
    database=postgres_db,
)
cur = conn.cursor()

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
        cur.execute(
            """
                INSERT INTO lecturers(first_name, last_name) VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
            """,
            (name, surname),
        )
conn.commit()

# inserting courses
for course in courses_lecturers_df["Courses"]:
    cur.execute(
        """
            INSERT INTO courses(name) VALUES (%s)
            ON CONFLICT DO NOTHING;
        """,
        (course,),
    )
conn.commit()

# inserting junction table courses_lecturers
rows = []
for _, row in courses_lecturers_df.iterrows():
    course = row["Courses"]
    for lec in row["Lecturers"]:
        first_name, last_name = lec[0], " ".join(lec[1:])
        rows.append((course, first_name, last_name))

for course, first_name, last_name in rows:
    cur.execute("SELECT id FROM courses WHERE name=%s", (course,))
    course_id = cur.fetchone()[0]  # type:ignore
    cur.execute(
        "SELECT id FROM lecturers WHERE first_name=%s AND last_name=%s",
        (first_name, last_name),
    )
    lecturer_id = cur.fetchone()[0]  # type:ignore
    cur.execute(
        "INSERT INTO courses_lecturers (course_id, lecturer_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (course_id, lecturer_id),
    )
conn.commit()

# inserting into groups table
cur.execute(
    """
        INSERT INTO groups(code, graduation_date) VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
    """,
    ("cohort1", "2025-07-01"),
)
conn.commit()

# inserting into groups table
cur.execute("""SELECT id FROM courses;""")
courses_ids = [i[0] for i in cur.fetchall()]
for course_id in courses_ids:
    cur.execute(
        """
            INSERT INTO groups_courses(group_code, course_id) VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
        """,
        ("cohort1", course_id),
    )
conn.commit()

cur.close()
conn.close()
