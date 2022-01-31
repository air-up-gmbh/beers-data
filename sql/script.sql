SELECT *
FROM beer b
         INNER JOIN ingredients_hops h on
    b.id = h.beer_id
         INNER JOIN ingredients_malt m on
    b.id = m.beer_id;


-- In SQL write a query to return the beer with most possible
-- “food_pairing”. Return the average “abv” and tell us shortly what to
-- keep in mind if you normalize the data.

select ROUND(AVG(abv), 2) as avg_abv from beer;

-----------------------------------------

SELECT b.id   as beer_id,
       b.name as name,
       f.name as food_pairing,
       b.abv  as abv
FROM beer b
         INNER JOIN beer_food_pairing_mapping m on
    b.id = m.beer_id
         INNER JOIN food_pairing f on
    f.id = m.food_pairing_id
WHERE beer_id in (
    select br.beer_id
    from (select bm.beer_id, dense_rank() over(order by bm.beer_count desc) as beer_rank
          from (select beer_id, count(*) beer_count
                from beer_food_pairing_mapping
                group by beer_id
                order by count(*)) bm) br
    where br.beer_rank = 1
);

-- --------------------------------------------

select beer_id, count(*) beer_count
from beer_food_pairing_mapping
group by beer_id
order by count(*);

select bm.beer_id, dense_rank() over(order by bm.beer_count desc) as beer_rank
from (select beer_id, count(*) beer_count from beer_food_pairing_mapping group by beer_id order by count(*)) bm;


sudo docker cp f851172d39c4:/var/lib/postgresql/ /home/jay/workspace/air-up/beers-data/sql/pg_backup.bak