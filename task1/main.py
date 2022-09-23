import json
from entity import student, room
from pymysql import connect, Error
from sql.sql_config import *


def read_json(json_file):
    with open(json_file) as file:
        file = json.load(file)
    return file


def parse_student_json_to_list(students_json):
    students = []
    for student_line in read_json("data/students.json"):
        students.append(student.Student(student_line["birthday"], student_line["id"], student_line["name"],
                                        student_line["room"], student_line["sex"]))
    return students


def parse_room_json_to_list(rooms_json):
    rooms = []
    for room_line in read_json(rooms_json):
        rooms.append(room.Room(room_line["id"], room_line["name"]))
    return rooms


def insert_students_to_db(students):
    connection = connect(host=db_host, user=db_user, password=db_password)
    cursor = connection.cursor()
    with open(insert_students) as script:
        queries = script.read().split(query_delimiter)
        queries.remove('')
        cursor.execute(queries[0])
    try:
        for student in students:
            cursor.execute(queries[1], (student.id, student.name, student.birthday, student.room, student.sex))
    except Error as e:
        print(e)
    finally:
        cursor.close()
        connection.commit()
        connection.close()


def insert_rooms_to_db(rooms):
    connection = connect(host=db_host, user=db_user, password=db_password)
    cursor = connection.cursor()
    with open(insert_rooms) as script:
        queries = script.read().split(query_delimiter)
        queries.remove('')
        cursor.execute(queries[0])
    try:
        for room in rooms:
            cursor.execute(queries[1], (room.id, room.name))
    except Error as e:
        print(e)
    finally:
        cursor.close()
        connection.commit()
        connection.close()


def get_rooms_with_student_amount():
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            with open(select_rooms_with_student_amount) as script:
                queries = script.read().split(query_delimiter)
                cursor.execute(queries[0])
                cursor.execute(queries[1])
                rooms_with_students_amount = cursor.fetchall()
                rooms_with_students_amount_list = []
                for room_with_student in rooms_with_students_amount:
                    rooms_with_students_amount_list.append(
                        [room.Room(room_with_student[0], room_with_student[1]), room_with_student[2]])
    finally:
        connection.close()
    return rooms_with_students_amount_list


def get_rooms_with_small_average_age():
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            with open(select_rooms_with_small_average_age) as script:
                queries = script.read().split(query_delimiter)
                cursor.execute(queries[0])
                cursor.execute(queries[1])
                rooms_with_small_average_age = cursor.fetchall()
                rooms_with_small_average_age_list = []
                for rooms_with_small_average_age_line in rooms_with_small_average_age:
                    rooms_with_small_average_age_list.append(
                        room.Room(rooms_with_small_average_age_line[0], rooms_with_small_average_age_line[1]))
    except Error as e:
        print(e)
    finally:
        connection.close()
    return rooms_with_small_average_age_list


def get_rooms_with_big_age_difference():
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            with open(select_rooms_with_big_age_difference) as script:
                queries = script.read().split(query_delimiter)
                cursor.execute(queries[0])
                cursor.execute(queries[1])
                rooms_with_big_age_difference = cursor.fetchall()
                rooms_with_big_age_difference_list = []
                for line in rooms_with_big_age_difference:
                    rooms_with_big_age_difference_list.append(room.Room(line[0], line[1]))
    except Error as e:
        print(e)
    finally:
        connection.close()
    return rooms_with_big_age_difference_list


def get_room_with_m_and_f():
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        with connection.cursor() as cursor:
            with open(select_rooms_with_m_and_f) as script:
                queries = script.read().split(query_delimiter)
                cursor.execute(queries[0])
                cursor.execute(queries[1])
                result = cursor.fetchall()
                room_with_m_and_f_list = []
                for line in result:
                    room_with_m_and_f_list.append(room.Room(line[0], line[1]))
    except Error as e:
        print(e)
    finally:
        connection.close()
    return room_with_m_and_f_list


if __name__ == '__main__':
    students = parse_student_json_to_list("data/students.json")
    rooms = parse_room_json_to_list("data/rooms.json")
    insert_rooms_to_db(rooms)
    insert_students_to_db(students)
    for room_with_student_amount in get_rooms_with_student_amount():
        print('room id: ' + str(room_with_student_amount[0].id) + ', name: ' + room_with_student_amount[
            0].name + ', students amount: ' + str(room_with_student_amount[1]))
    for room_with_average_age in get_rooms_with_small_average_age():
        print('room id: ' + str(room_with_average_age.id) + ', room name: ' + room_with_average_age.name)
    for room_with_big_age_difference in get_rooms_with_big_age_difference():
        print('room id: ' + str(room_with_big_age_difference.id) + ', room name: ' + room_with_big_age_difference.name)
    lists= get_room_with_m_and_f()

    for ro in get_room_with_m_and_f():
        print('room id: ' + str(ro.id) + ', room name: ' + ro.name)
