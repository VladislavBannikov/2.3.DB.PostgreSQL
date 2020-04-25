import psycopg2 as pg
import json
from pprint import pprint


def create_db(): # создает таблицы
    student_sql = """
        create table if not exists student(
        id serial primary key,
        name varchar(100) not null,
        gpa numeric(4,2),
        birth timestamptz);
        """
    course_sql = """
        create table if not exists course(
        id serial primary key,
        name varchar(100) not null
        );        
        """
    course_student_sql = """
        create table if not exists course_student(
        id serial primary key,
        id_student INTEGER REFERENCES student(id) not null,
        id_course INTEGER REFERENCES course(id) not null
        );
        """
    queries = {'student':student_sql, 'course': course_sql, 'course_student': course_student_sql}

    with conn.cursor() as cur:
        for table_name in reversed(list(queries.keys())):       # DROP all tables
            cur.execute(f'DROP TABLE if exists "{table_name}";')
        for q in queries.values():                              # CREATE all tables
            cur.execute(q)

        cur.execute("INSERT INTO course(name) VALUES ('js'),('dj'),('py');")
        conn.commit()


def get_students(course_id): # возвращает студентов определенного курса
    with conn.cursor() as cur:
        q = f"select st.name, st.gpa, st.birth from student as st \
                join course_student as cs on cs.id_student = st.id where cs.id_course = '{course_id}'"
        cur.execute(q)
        return cur.fetchall()


def add_students(course_id, students): # создает студентов и записывает их на курс
    with conn.cursor() as cur:
        for st in students:
            name = st.get('name')
            gpa = st.get('gpa')
            birth = st.get('birth')
            cur.execute(f"INSERT INTO student(name,gpa,birth) VALUES ('{name}','{gpa}',to_timestamp('{birth}','YYYY-MM-DD')) RETURNING id")
            st_id = cur.fetchone()[0]
            cur.execute(f"INSERT INTO course_student(id_student, id_course) VALUES ('{st_id}','{course_id}')")
    conn.commit()


def add_student(st): # просто создает студента
    with conn.cursor() as cur:
        name = st.get('name')
        gpa = st.get('gpa')
        birth = st.get('birth')
        cur.execute(f"INSERT INTO student(name,gpa,birth) VALUES ('{name}','{gpa}',to_timestamp('{birth}','YYYY-MM-DD')) RETURNING id")
    conn.commit()


def get_student(student_id):
    with conn.cursor() as cur:
        q = f"select st.name, st.gpa, st.birth, c.name from student as st \
                join course_student as cs on cs.id_student = st.id \
                join course as c on c.id = cs.id_course where st.id = '{student_id}'"
        cur.execute(q)
        return cur.fetchall()


if __name__ == '__main__':
    with pg.connect(dbname='netology',
                    user='netology_user',
                    password='1'
                    ) as conn:
        create_db()
        with open("fixtures/students.json", encoding='utf8') as f:
            students = json.load(f)

        add_students(1,students[:2])
        add_students(2, students[2:])
        add_student(students[0])
        print("====get_student======")
        print(get_student(3))
        print("====get_students======")
        pprint(get_students(2))

        ## подстановка не работает, пример:
        # with conn.cursor() as cur:
        #     cur.execute("DROP TABLE if exists %s;", ("course",))

        #     cur.execute("DROP TABLE if exists %s;", ("course",))
        # psycopg2.errors.SyntaxError: ОШИБКА:  ошибка синтаксиса (примерное положение: "'course'")
        # LINE 1: DROP TABLE if exists 'course';
