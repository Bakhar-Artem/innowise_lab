use booking_db;
select room_id, room_name
from (select room_id, room_name
      from room_table
               left join student_table st
                         on room_table.room_id = st.student_room
      group by room_id, student_sex) as cte
group by room_id, room_name
having count(room_id) = 2;