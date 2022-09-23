use booking_db;
insert into student_table(student_id, student_name, student_birthday, student_room, student_sex)
values (%s, %s, %s, %s, %s);