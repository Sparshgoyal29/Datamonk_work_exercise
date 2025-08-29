INSTALL sqlite_scanner;
LOAD sqlite_scanner;
ATTACH 'sakila.db' AS sakila (TYPE sqlite);
USE sakila;

-- Part 1 Join Challenge
WITH customer_film_spend AS (
  SELECT r.customer_id, i.film_id, SUM(p.amount) AS total_spent
  FROM sakila.rental r
  JOIN sakila.payment p ON r.rental_id = p.rental_id
  JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
  GROUP BY r.customer_id, i.film_id
)
SELECT f.title AS film_title,
       c2.name AS category_name,
       cu.first_name || ' ' || cu.last_name AS customer_name,
       ci.city || ', ' || co.country AS store_location,
       r.rental_date,
       cf.total_spent AS customer_total_for_film
FROM sakila.rental r
JOIN sakila.inventory i ON r.inventory_id = i.inventory_id
JOIN sakila.film f ON i.film_id = f.film_id
JOIN sakila.film_category fc ON f.film_id = fc.film_id
JOIN sakila.category c2 ON fc.category_id = c2.category_id
JOIN sakila.customer cu ON r.customer_id = cu.customer_id
JOIN sakila.store s ON cu.store_id = s.store_id
JOIN sakila.address a ON s.address_id = a.address_id
JOIN sakila.city ci ON a.city_id = ci.city_id
JOIN sakila.country co ON ci.country_id = co.country_id
JOIN customer_film_spend cf ON cf.customer_id = cu.customer_id AND cf.film_id = f.film_id
LIMIT 20;
