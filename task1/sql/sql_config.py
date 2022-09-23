# db connection
db_host = 'localhost'
db_user = 'bakhar'
db_password = '1111'

# db setup

create_db = ['sql/ddl/db/booking.sql', 'sql/ddl/table/room_table.sql', 'sql/ddl/table/student_table.sql']

# sql inserts

insert_rooms = 'sql/dml/insert/room_insert.sql'
insert_students = 'sql/dml/insert/student_insert.sql'

# sql selects
select_rooms_with_student_amount = 'sql/dml/select/room_with_students_count_select.sql'
select_rooms_with_small_average_age = 'sql/dml/select/room_with_small_average_age_select.sql'
select_rooms_with_big_age_difference = 'sql/dml/select/room_with_big_age_difference_select.sql'
select_rooms_with_m_and_f = 'sql/dml/select/room_with_m_and_f_select.sql'

# delimiter
query_delimiter = ';'
