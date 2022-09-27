select count(film_id) as id_, category."name" from film_category JOIN category ON category.category_id = film_category.category_id group by(category."name")
ORDER by id_ desc


select first_name,last_name, count(rental.rental_id) as rent_amount from actor
join film_actor ON film_actor.actor_id = actor.actor_id
JOIN inventory on inventory.film_id = film_actor.film_id
join rental on rental.inventory_id = inventory.inventory_id
group by first_name,last_name order by rent_amount desc limit 10
 

select category.name,sum(payment.amount) as amount_ from category 
join film_category ON film_category.category_id = category.category_id
join film ON film.film_id = film_category.film_id
join inventory on inventory.film_id = film.film_id
join rental on rental.inventory_id = inventory.inventory_id
join payment on payment.rental_id = rental.rental_id
group by category.name order by amount_ desc limit 1


select film.title from film 
left join inventory on film.film_id = inventory.film_id 
where inventory.inventory_id isnull 
group by film.title

select first_name, last_name from
(select first_name, last_name, rank() over(
	order by count(actor.actor_id) desc) rank_count
from actor 
join film_actor on actor.actor_id = film_actor.actor_id
join film_category on film_category.film_id = film_actor.film_id
join category on category.category_id = film_category.category_id
where category.name = 'Children'
GROUP by actor.actor_id) as tmp 
where tmp.rank_count<3