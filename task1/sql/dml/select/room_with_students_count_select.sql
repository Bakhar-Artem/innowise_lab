use booking_db;
select room_id, room_name, count(room_id) students_amount
from room_table
         left join student_table st on room_table.room_id = st.student_room
group by room_id;