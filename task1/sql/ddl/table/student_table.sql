use booking_db;
drop table if exists student_table;
create table student_table
(
    student_id       int          not null primary key,
    student_name     varchar(255) not null,
    student_birthday datetime     not null,
    student_room     int          not null,
    student_sex      varchar(1),
    constraint fk_student_room foreign key (student_room) references room_table (room_id)
);
create index index_room_and_birthday on student_table (student_room, student_birthday);