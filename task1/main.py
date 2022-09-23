import argparse
import json
from entity import student, room
from pymysql import connect, Error
from sql.sql_config import *


def read_json(json_file):
    """
    Function to read data from json file
    :param json_file: path to json
    :return: read file
    """
    with open(json_file) as file:
        file = json.load(file)
    return file


def parse_student_json_to_list(students_json):
    """
    Function to parse json and fill student list
    :param students_json: path to json
    :return: list of students
    """
    students = []
    for student_line in read_json(students_json):
        students.append(student.Student(student_line["birthday"], student_line["id"], student_line["name"],
                                        student_line["room"], student_line["sex"]))
    return students


def parse_room_json_to_list(rooms_json):
    """
    Function to parse json and fill room list
    :param rooms_json: path to json
    :return: list of rooms
    """
    rooms = []
    for room_line in read_json(rooms_json):
        rooms.append(room.Room(room_line["id"], room_line["name"]))
    return rooms


def insert_students_to_db(students):
    """
    Function to fill student table in db
    :param students: list of students
    :return: none
    """
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
    """
    Function to fill student table in db
    :param rooms: list of rooms
    :return: none
    """
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
    """
    Function to get rooms with amount of people
    :return: list [room,amount]
    """
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
    """
    Function to get rooms where the smallest average people age
    :return: list of rooms
    """
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
    """
    Function to get rooms with the biggest difference people age
    :return: list of rooms
    """
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
    """
    Function to get rooms where male and female at time
    :return: list of rooms
    """
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


def get_args_from_command_line():
    """
    Parses command line arguments
    """
    parser = argparse.ArgumentParser(
        "Console utility to find top rooms by criteria")
    parser.add_argument("--students_path", type=str, nargs=1, help='students json path')
    parser.add_argument("--rooms_path", type=str, nargs=1, help='rooms json path')
    parser.add_argument("--format", type=str, nargs=1, help='choose xml or json format')
    return parser.parse_args()


def get_sp_get_movies_params(args_namespace):
    """
    process args from command line to execute saved procedure
    :param args_namespace: namespace with args from command line
    :return: list [student_path, rooms_path, format]
    """
    procedure_params = []
    if args_namespace.students_path:
        procedure_params.append(args_namespace.students_path[0])
    else:
        procedure_params.append(None)
    if args_namespace.rooms_path:
        procedure_params.append(args_namespace.rooms_path[0])
    else:
        procedure_params.append(None)
    if args_namespace.format:
        procedure_params.append(args_namespace.format[0])
    else:
        procedure_params.append(None)
    return procedure_params


def setup_db():
    """
    Function to set up db and tables init
    :return: none
    """
    connection = connect(host=db_host, user=db_user, password=db_password)
    try:
        for create_script in create_db:
            with open(create_script) as script:
                queries = script.read().split(query_delimiter)
                for query in queries:
                    if len(query) > 1 and query.strip():
                        with connection.cursor() as cursor:
                            cursor.execute(query)
    except Error as e:
        print(e)
    finally:
        connection.close()


def main():
    """
    main function to set up db from user params and print result
    :return: none
    """
    args_namespace = get_args_from_command_line()
    params = get_sp_get_movies_params(args_namespace)
    setup_db()
    student_json = params[0]
    room_json = params[1]
    insert_rooms_to_db(parse_room_json_to_list(room_json))
    insert_students_to_db(parse_student_json_to_list(student_json))
    user_format = params[2]
    if user_format == 'xml':
        return #to do
    else:
        print_json_output()
    return


def print_json_output():
    print("room list and people inside")
    rooms_list = get_rooms_with_student_amount()
    room_str_list = ['[\n']
    print('[')
    for rooms in rooms_list:
        print(
            '{\n\t{\n\t\troom_id: ' + str(rooms[0].id) + ',\n\t\troom_name: \"' + rooms[
                0].name + '\"\n\t},\n\tamount: ' + str(
                rooms[
                    1]) + '\n},')
    print(']')
    rooms_list = get_rooms_with_small_average_age()
    print('room list with average age')
    print('[')
    for rooms in rooms_list:
        print('{\n\troom_id: ' + str(rooms.id) + ',\n\troom_name: \"' + rooms.name + '\"\n},')
    print(']')
    print('room list with max age difference')
    print('[')
    for rooms in rooms_list:
        print('{\n\troom_id: ' + str(rooms.id) + ',\n\troom_name: \"' + rooms.name + '\"\n},')
    print(']')
    print('room list with male and female at a time')
    print('[')
    for rooms in rooms_list:
        print('{\n\troom_id: ' + str(rooms.id) + ',\n\troom_name: \"' + rooms.name + '\"\n},')
    print(']')


if __name__ == '__main__':
    main()
