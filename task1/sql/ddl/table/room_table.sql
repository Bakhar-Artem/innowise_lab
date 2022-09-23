use booking_db;
drop table if exists room_table;
create table room_table
(
    room_id   int          not null primary key,
    room_name varchar(255) not null
);