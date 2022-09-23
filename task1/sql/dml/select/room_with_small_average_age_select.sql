use booking_db;
select room_id,room_name
from room_table
         left join student_table st on room_table.room_id = st.student_room group by room_id order by avg(student_birthday) desc limit 5;